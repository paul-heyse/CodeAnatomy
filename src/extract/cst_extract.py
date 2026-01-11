"""Extract LibCST-derived structures into Arrow tables."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import libcst as cst
import pyarrow as pa
from libcst import helpers
from libcst.metadata import (
    BaseMetadataProvider,
    ByteSpanPositionProvider,
    ExpressionContextProvider,
    MetadataWrapper,
    QualifiedName,
    QualifiedNameProvider,
)

from extract.repo_scan import stable_id

SCHEMA_VERSION = 1

type Row = dict[str, object]


@dataclass(frozen=True)
class CSTExtractOptions:
    """Define LibCST extraction options."""

    repo_id: str | None = None
    repo_root: Path | None = None
    include_parse_manifest: bool = True
    include_parse_errors: bool = True
    include_name_refs: bool = True
    include_imports: bool = True
    include_callsites: bool = True
    include_defs: bool = True
    compute_expr_context: bool = True
    compute_qualified_names: bool = True


@dataclass(frozen=True)
class CSTExtractResult:
    """Hold extracted CST tables for manifests, errors, names, imports, and calls."""

    py_cst_parse_manifest: pa.Table
    py_cst_parse_errors: pa.Table
    py_cst_name_refs: pa.Table
    py_cst_imports: pa.Table
    py_cst_callsites: pa.Table
    py_cst_defs: pa.Table


QNAME_STRUCT = pa.struct([("name", pa.string()), ("source", pa.string())])
QNAME_LIST = pa.list_(QNAME_STRUCT)

PARSE_MANIFEST_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("encoding", pa.string()),
        ("default_indent", pa.string()),
        ("default_newline", pa.string()),
        ("has_trailing_newline", pa.bool_()),
        ("future_imports", pa.list_(pa.string())),
        ("module_name", pa.string()),
        ("package_name", pa.string()),
    ]
)

PARSE_ERRORS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("error_type", pa.string()),
        ("message", pa.string()),
        ("raw_line", pa.int32()),
        ("raw_column", pa.int32()),
    ]
)

NAME_REFS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("name_ref_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("name", pa.string()),
        ("expr_ctx", pa.string()),
        ("bstart", pa.int64()),
        ("bend", pa.int64()),
    ]
)

IMPORTS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("import_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("kind", pa.string()),
        ("module", pa.string()),
        ("relative_level", pa.int32()),
        ("name", pa.string()),
        ("asname", pa.string()),
        ("is_star", pa.bool_()),
        ("stmt_bstart", pa.int64()),
        ("stmt_bend", pa.int64()),
        ("alias_bstart", pa.int64()),
        ("alias_bend", pa.int64()),
    ]
)

CALLSITES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("call_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("call_bstart", pa.int64()),
        ("call_bend", pa.int64()),
        ("callee_bstart", pa.int64()),
        ("callee_bend", pa.int64()),
        ("callee_dotted", pa.string()),
        ("callee_qnames", QNAME_LIST),
    ]
)

DEFS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("def_id", pa.string()),
        ("container_def_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("kind", pa.string()),
        ("name", pa.string()),
        ("def_bstart", pa.int64()),
        ("def_bend", pa.int64()),
        ("name_bstart", pa.int64()),
        ("name_bend", pa.int64()),
        ("qnames", QNAME_LIST),
    ]
)


@dataclass(frozen=True)
class CSTFileContext:
    """Per-file context for CST extraction."""

    file_id: str
    path: str
    file_sha256: str | None
    options: CSTExtractOptions


@dataclass
class CSTExtractContext:
    """Shared extraction context and output buffers."""

    options: CSTExtractOptions
    manifest_rows: list[Row]
    error_rows: list[Row]
    name_ref_rows: list[Row]
    import_rows: list[Row]
    call_rows: list[Row]
    def_rows: list[Row]


def _empty(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


def _calculate_module_and_package(repo_root: Path, abs_path: Path) -> tuple[str | None, str | None]:
    try:
        result = helpers.calculate_module_and_package(str(repo_root), str(abs_path))
    except (OSError, ValueError):
        return None, None
    else:
        return result.name, result.package


def _row_bytes(rf: dict[str, object]) -> bytes | None:
    data = rf.get("bytes")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    text = rf.get("text")
    if not isinstance(text, str) or not text:
        return None
    encoding_value = rf.get("encoding")
    encoding = encoding_value if isinstance(encoding_value, str) else "utf-8"
    return text.encode(encoding, errors="replace")


def _parse_module(
    data: bytes,
    *,
    file_id: str,
    path: str,
    file_sha256: str | None,
) -> tuple[cst.Module | None, Row | None]:
    try:
        return cst.parse_module(data), None
    except cst.ParserSyntaxError as exc:
        return (
            None,
            {
                "schema_version": SCHEMA_VERSION,
                "file_id": file_id,
                "path": path,
                "file_sha256": file_sha256,
                "error_type": type(exc).__name__,
                "message": str(exc),
                "raw_line": int(getattr(exc, "raw_line", 0) or 0),
                "raw_column": int(getattr(exc, "raw_column", 0) or 0),
            },
        )


def _module_package_names(
    rf: dict[str, object],
    *,
    options: CSTExtractOptions,
    path: str,
) -> tuple[str | None, str | None]:
    if options.repo_root is None:
        return None, None
    abs_path_value = rf.get("abs_path")
    if isinstance(abs_path_value, Path):
        abs_path = abs_path_value
    elif isinstance(abs_path_value, str):
        abs_path = Path(abs_path_value)
    else:
        abs_path = Path(path)
    return _calculate_module_and_package(options.repo_root, abs_path)


def _parse_or_record_error(
    data: bytes,
    *,
    file_id: str,
    path: str,
    file_sha256: str | None,
    ctx: CSTExtractContext,
) -> cst.Module | None:
    module, err = _parse_module(data, file_id=file_id, path=path, file_sha256=file_sha256)
    if module is None:
        if err is not None and ctx.options.include_parse_errors:
            ctx.error_rows.append(err)
        return None
    return module


def _manifest_row(
    ctx: CSTFileContext,
    module: cst.Module,
    *,
    module_name: str | None,
    package_name: str | None,
) -> Row:
    return {
        "schema_version": SCHEMA_VERSION,
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
        "encoding": getattr(module, "encoding", None),
        "default_indent": getattr(module, "default_indent", None),
        "default_newline": getattr(module, "default_newline", None),
        "has_trailing_newline": bool(getattr(module, "has_trailing_newline", False)),
        "future_imports": list(getattr(module, "future_imports", []) or []),
        "module_name": module_name,
        "package_name": package_name,
    }


def _resolve_metadata_maps(
    wrapper: MetadataWrapper,
    options: CSTExtractOptions,
) -> tuple[
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, object],
    Mapping[cst.CSTNode, Collection[QualifiedName]],
]:
    providers: set[type[BaseMetadataProvider[object]]] = {ByteSpanPositionProvider}
    if options.compute_expr_context and options.include_name_refs:
        providers.add(ExpressionContextProvider)
    if options.compute_qualified_names and (options.include_callsites or options.include_defs):
        providers.add(QualifiedNameProvider)

    resolved = wrapper.resolve_many(providers)
    return (
        resolved.get(ByteSpanPositionProvider, {}),
        resolved.get(ExpressionContextProvider, {}),
        cast(
            "Mapping[cst.CSTNode, Collection[QualifiedName]]",
            resolved.get(QualifiedNameProvider, {}),
        ),
    )


def _visit_with_collector(
    module: cst.Module,
    *,
    file_ctx: CSTFileContext,
    ctx: CSTExtractContext,
) -> None:
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    span_map, ctx_map, qn_map = _resolve_metadata_maps(wrapper, ctx.options)
    collector = CSTCollector(
        file_ctx=file_ctx,
        extract_ctx=ctx,
        span_map=span_map,
        ctx_map=ctx_map,
        qn_map=qn_map,
    )
    wrapper.visit(collector)


class CSTCollector(cst.CSTVisitor):
    """Collect CST-derived rows for a single file."""

    def __init__(
        self,
        *,
        file_ctx: CSTFileContext,
        extract_ctx: CSTExtractContext,
        span_map: Mapping[cst.CSTNode, object],
        ctx_map: Mapping[cst.CSTNode, object],
        qn_map: Mapping[cst.CSTNode, Collection[QualifiedName]],
    ) -> None:
        self._file_ctx = file_ctx
        self._options = extract_ctx.options
        self._span_map = span_map
        self._ctx_map = ctx_map
        self._qn_map = qn_map
        self._name_ref_rows = extract_ctx.name_ref_rows
        self._import_rows = extract_ctx.import_rows
        self._call_rows = extract_ctx.call_rows
        self._def_rows = extract_ctx.def_rows
        self._skip_name_nodes: set[cst.Name] = set()
        self._def_stack: list[str] = []

    def _span(self, node: cst.CSTNode) -> tuple[int, int]:
        sp = self._span_map.get(node)
        if sp is None:
            return 0, 0
        start = int(getattr(sp, "start", 0))
        length = int(getattr(sp, "length", 0))
        return start, start + length

    def _qnames(self, node: cst.CSTNode) -> list[Row]:
        qset = self._qn_map.get(node)
        if not qset:
            return []
        qualified = sorted(qset, key=lambda q: (q.name, str(q.source)))
        return [{"name": q.name, "source": str(q.source)} for q in qualified]

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool | None:
        """Collect function definition rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_defs:
            return True
        def_bstart, def_bend = self._span(node)
        name_bstart, name_bend = self._span(node.name)
        self._skip_name_nodes.add(node.name)

        container = self._def_stack[-1] if self._def_stack else None
        def_kind = "async_function" if node.asynchronous is not None else "function"
        def_id = stable_id(
            "cst_def", self._file_ctx.file_id, def_kind, str(def_bstart), str(def_bend)
        )

        self._def_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "def_id": def_id,
                "container_def_id": container,
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "kind": def_kind,
                "name": node.name.value,
                "def_bstart": def_bstart,
                "def_bend": def_bend,
                "name_bstart": name_bstart,
                "name_bend": name_bend,
                "qnames": self._qnames(node),
            }
        )
        self._def_stack.append(def_id)
        return True

    def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
        """Finalize function definition collection."""
        _ = original_node
        if self._options.include_defs and self._def_stack:
            self._def_stack.pop()

    def visit_ClassDef(self, node: cst.ClassDef) -> bool | None:
        """Collect class definition rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_defs:
            return True
        def_bstart, def_bend = self._span(node)
        name_bstart, name_bend = self._span(node.name)
        self._skip_name_nodes.add(node.name)

        container = self._def_stack[-1] if self._def_stack else None
        def_kind = "class"
        def_id = stable_id(
            "cst_def", self._file_ctx.file_id, def_kind, str(def_bstart), str(def_bend)
        )

        self._def_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "def_id": def_id,
                "container_def_id": container,
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "kind": def_kind,
                "name": node.name.value,
                "def_bstart": def_bstart,
                "def_bend": def_bend,
                "name_bstart": name_bstart,
                "name_bend": name_bend,
                "qnames": self._qnames(node),
            }
        )
        self._def_stack.append(def_id)
        return True

    def leave_ClassDef(self, original_node: cst.ClassDef) -> None:
        """Finalize class definition collection."""
        _ = original_node
        if self._options.include_defs and self._def_stack:
            self._def_stack.pop()

    def visit_Name(self, node: cst.Name) -> bool | None:
        """Collect name reference rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_name_refs:
            return True
        if node in self._skip_name_nodes:
            return True
        bstart, bend = self._span(node)
        expr_ctx = None
        if self._options.compute_expr_context:
            ctx = self._ctx_map.get(node)
            expr_ctx = str(ctx) if ctx is not None else None

        self._name_ref_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "name_ref_id": stable_id(
                    "cst_name_ref", self._file_ctx.file_id, str(bstart), str(bend)
                ),
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "name": node.value,
                "expr_ctx": expr_ctx,
                "bstart": bstart,
                "bend": bend,
            }
        )
        return True

    def visit_Import(self, node: cst.Import) -> bool | None:
        """Collect import rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_imports:
            return True
        stmt_bstart, stmt_bend = self._span(node)
        for alias in node.names:
            alias_bstart, alias_bend = self._span(alias)
            name = helpers.get_full_name_for_node(alias.name) or getattr(alias.name, "value", None)

            asname: str | None = None
            if isinstance(alias.asname, cst.AsName):
                asname = helpers.get_full_name_for_node(alias.asname.name) or getattr(
                    alias.asname.name, "value", None
                )

            self._import_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "import_id": stable_id(
                        "cst_import",
                        self._file_ctx.file_id,
                        "import",
                        str(alias_bstart),
                        str(alias_bend),
                    ),
                    "file_id": self._file_ctx.file_id,
                    "path": self._file_ctx.path,
                    "file_sha256": self._file_ctx.file_sha256,
                    "kind": "import",
                    "module": None,
                    "relative_level": 0,
                    "name": name,
                    "asname": asname,
                    "is_star": False,
                    "stmt_bstart": stmt_bstart,
                    "stmt_bend": stmt_bend,
                    "alias_bstart": alias_bstart,
                    "alias_bend": alias_bend,
                }
            )
        return True

    def visit_ImportFrom(self, node: cst.ImportFrom) -> bool | None:
        """Collect from-import rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_imports:
            return True
        stmt_bstart, stmt_bend = self._span(node)

        module = None
        if node.module is not None:
            module = helpers.get_full_name_for_node(node.module) or getattr(
                node.module, "value", None
            )

        relative = node.relative
        relative_level = len(relative) if isinstance(relative, Sequence) else 0

        if isinstance(node.names, cst.ImportStar):
            alias_bstart, alias_bend = self._span(node.names)
            self._import_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "import_id": stable_id(
                        "cst_import",
                        self._file_ctx.file_id,
                        "importfrom",
                        str(alias_bstart),
                        str(alias_bend),
                    ),
                    "file_id": self._file_ctx.file_id,
                    "path": self._file_ctx.path,
                    "file_sha256": self._file_ctx.file_sha256,
                    "kind": "importfrom",
                    "module": module,
                    "relative_level": relative_level,
                    "name": "*",
                    "asname": None,
                    "is_star": True,
                    "stmt_bstart": stmt_bstart,
                    "stmt_bend": stmt_bend,
                    "alias_bstart": alias_bstart,
                    "alias_bend": alias_bend,
                }
            )
            return True

        for alias in node.names:
            alias_bstart, alias_bend = self._span(alias)
            name = helpers.get_full_name_for_node(alias.name) or getattr(alias.name, "value", None)

            asname: str | None = None
            if isinstance(alias.asname, cst.AsName):
                asname = helpers.get_full_name_for_node(alias.asname.name) or getattr(
                    alias.asname.name, "value", None
                )

            self._import_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "import_id": stable_id(
                        "cst_import",
                        self._file_ctx.file_id,
                        "importfrom",
                        str(alias_bstart),
                        str(alias_bend),
                    ),
                    "file_id": self._file_ctx.file_id,
                    "path": self._file_ctx.path,
                    "file_sha256": self._file_ctx.file_sha256,
                    "kind": "importfrom",
                    "module": module,
                    "relative_level": relative_level,
                    "name": name,
                    "asname": asname,
                    "is_star": False,
                    "stmt_bstart": stmt_bstart,
                    "stmt_bend": stmt_bend,
                    "alias_bstart": alias_bstart,
                    "alias_bend": alias_bend,
                }
            )
        return True

    def visit_Call(self, node: cst.Call) -> bool | None:
        """Collect callsite rows.

        Returns
        -------
        bool | None
            True to continue CST traversal.
        """
        if not self._options.include_callsites:
            return True
        call_bstart, call_bend = self._span(node)
        callee_bstart, callee_bend = self._span(node.func)

        callee_dotted = helpers.get_full_name_for_node(node.func)

        qnames = self._qnames(node.func)
        if not qnames:
            qnames = self._qnames(node)

        self._call_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "call_id": stable_id(
                    "cst_call", self._file_ctx.file_id, str(call_bstart), str(call_bend)
                ),
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "call_bstart": call_bstart,
                "call_bend": call_bend,
                "callee_bstart": callee_bstart,
                "callee_bend": callee_bend,
                "callee_dotted": callee_dotted,
                "callee_qnames": qnames,
            }
        )
        return True


def _extract_cst_for_row(rf: dict[str, object], ctx: CSTExtractContext) -> None:
    file_id_val = rf.get("file_id")
    path_val = rf.get("path")
    if file_id_val is None or path_val is None:
        return
    file_id = str(file_id_val)
    path = str(path_val)
    file_sha256 = rf.get("file_sha256")
    if not isinstance(file_sha256, str):
        file_sha256 = None

    data = _row_bytes(rf)
    if data is None:
        return

    module = _parse_or_record_error(
        data,
        file_id=file_id,
        path=path,
        file_sha256=file_sha256,
        ctx=ctx,
    )
    if module is None:
        return

    module_name, package_name = _module_package_names(rf, options=ctx.options, path=path)
    file_ctx = CSTFileContext(
        file_id=file_id,
        path=path,
        file_sha256=file_sha256,
        options=ctx.options,
    )
    if ctx.options.include_parse_manifest:
        ctx.manifest_rows.append(
            _manifest_row(file_ctx, module, module_name=module_name, package_name=package_name)
        )

    _visit_with_collector(module, file_ctx=file_ctx, ctx=ctx)


def extract_cst(repo_files: pa.Table, options: CSTExtractOptions | None = None) -> CSTExtractResult:
    """Extract LibCST-derived structures from repo files.

    Returns
    -------
    CSTExtractResult
        Tables derived from LibCST parsing and metadata providers.
    """
    options = options or CSTExtractOptions()

    manifest_rows: list[Row] = []
    error_rows: list[Row] = []
    name_ref_rows: list[Row] = []
    import_rows: list[Row] = []
    call_rows: list[Row] = []
    def_rows: list[Row] = []

    ctx = CSTExtractContext(
        options=options,
        manifest_rows=manifest_rows,
        error_rows=error_rows,
        name_ref_rows=name_ref_rows,
        import_rows=import_rows,
        call_rows=call_rows,
        def_rows=def_rows,
    )

    for rf in repo_files.to_pylist():
        _extract_cst_for_row(rf, ctx)

    t_manifest = (
        pa.Table.from_pylist(manifest_rows, schema=PARSE_MANIFEST_SCHEMA)
        if manifest_rows
        else _empty(PARSE_MANIFEST_SCHEMA)
    )
    t_errs = (
        pa.Table.from_pylist(error_rows, schema=PARSE_ERRORS_SCHEMA)
        if error_rows
        else _empty(PARSE_ERRORS_SCHEMA)
    )
    t_names = (
        pa.Table.from_pylist(name_ref_rows, schema=NAME_REFS_SCHEMA)
        if name_ref_rows
        else _empty(NAME_REFS_SCHEMA)
    )
    t_imports = (
        pa.Table.from_pylist(import_rows, schema=IMPORTS_SCHEMA)
        if import_rows
        else _empty(IMPORTS_SCHEMA)
    )
    t_calls = (
        pa.Table.from_pylist(call_rows, schema=CALLSITES_SCHEMA)
        if call_rows
        else _empty(CALLSITES_SCHEMA)
    )
    t_defs = pa.Table.from_pylist(def_rows, schema=DEFS_SCHEMA) if def_rows else _empty(DEFS_SCHEMA)

    return CSTExtractResult(
        py_cst_parse_manifest=t_manifest,
        py_cst_parse_errors=t_errs,
        py_cst_name_refs=t_names,
        py_cst_imports=t_imports,
        py_cst_callsites=t_calls,
        py_cst_defs=t_defs,
    )


def extract_cst_tables(
    *,
    repo_root: str | None,
    repo_files: pa.Table,
    ctx: object | None = None,
) -> dict[str, pa.Table]:
    """Extract CST tables as a name-keyed bundle.

    Parameters
    ----------
    repo_root:
        Optional repository root for module/package derivation.
    repo_files:
        Repo files table.
    ctx:
        Execution context (unused).

    Returns
    -------
    dict[str, pyarrow.Table]
        Extracted CST tables keyed by output name.
    """
    _ = ctx
    options = CSTExtractOptions(repo_root=Path(repo_root) if repo_root else None)
    result = extract_cst(repo_files, options=options)
    return {
        "cst_parse_manifest": result.py_cst_parse_manifest,
        "cst_parse_errors": result.py_cst_parse_errors,
        "cst_name_refs": result.py_cst_name_refs,
        "cst_imports": result.py_cst_imports,
        "cst_callsites": result.py_cst_callsites,
        "cst_defs": result.py_cst_defs,
    }
