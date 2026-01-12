"""Extract LibCST-derived structures into Arrow tables."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import libcst as cst
from libcst import helpers
from libcst.metadata import (
    BaseMetadataProvider,
    ByteSpanPositionProvider,
    ExpressionContextProvider,
    MetadataWrapper,
    QualifiedName,
    QualifiedNameProvider,
)

import arrowdsl.pyarrow_core as pa
from arrowdsl.column_ops import set_or_append_column
from arrowdsl.compute import pc
from arrowdsl.empty import empty_table
from arrowdsl.id_specs import HashSpec
from arrowdsl.ids import hash_column_values
from arrowdsl.nested import build_list_array, build_list_of_structs
from arrowdsl.pyarrow_protocols import TableLike
from arrowdsl.schema_ops import SchemaTransform
from extract.file_context import FileContext, iter_file_contexts
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import call_span_bundle, file_identity_bundle
from schema_spec.registry import GLOBAL_SCHEMA_REGISTRY

SCHEMA_VERSION = 1

type Row = dict[str, object]


def _offsets_start() -> list[int]:
    return [0]


def _valid_mask(table: TableLike, cols: Sequence[str]) -> object:
    mask = pc.is_valid(table[cols[0]])
    for col in cols[1:]:
        mask = pc.and_(mask, pc.is_valid(table[col]))
    return mask


def _apply_hash_column(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> TableLike:
    hashed = hash_column_values(table, spec=spec)
    out_col = spec.out_col or f"{spec.prefix}_id"
    if required:
        mask = _valid_mask(table, required)
        hashed = pc.if_else(mask, hashed, pa.scalar(None, type=hashed.type))
    return set_or_append_column(table, out_col, hashed)


def _append_string_list(
    offsets: list[int],
    values: list[str | None],
    items: Sequence[object],
) -> None:
    for item in items:
        if item is None:
            values.append(None)
        elif isinstance(item, str):
            values.append(item)
        else:
            values.append(str(item))
    offsets.append(len(values))


def _append_qname_list(
    offsets: list[int],
    names: list[str],
    sources: list[str],
    qnames: Sequence[tuple[str, str]],
) -> None:
    for name, source in qnames:
        names.append(name)
        sources.append(source)
    offsets.append(len(names))


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
    include_type_exprs: bool = True
    compute_expr_context: bool = True
    compute_qualified_names: bool = True


@dataclass(frozen=True)
class CSTExtractResult:
    """Hold extracted CST tables for manifests, errors, names, imports, and calls."""

    py_cst_parse_manifest: TableLike
    py_cst_parse_errors: TableLike
    py_cst_name_refs: TableLike
    py_cst_imports: TableLike
    py_cst_callsites: TableLike
    py_cst_defs: TableLike
    py_cst_type_exprs: TableLike


QNAME_STRUCT = pa.struct([("name", pa.string()), ("source", pa.string())])
QNAME_LIST = pa.list_(QNAME_STRUCT)

PARSE_MANIFEST_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_parse_manifest_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="encoding", dtype=pa.string()),
            ArrowFieldSpec(name="default_indent", dtype=pa.string()),
            ArrowFieldSpec(name="default_newline", dtype=pa.string()),
            ArrowFieldSpec(name="has_trailing_newline", dtype=pa.bool_()),
            ArrowFieldSpec(name="future_imports", dtype=pa.list_(pa.string())),
            ArrowFieldSpec(name="module_name", dtype=pa.string()),
            ArrowFieldSpec(name="package_name", dtype=pa.string()),
        ],
    )
)

PARSE_ERRORS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_parse_errors_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="error_type", dtype=pa.string()),
            ArrowFieldSpec(name="message", dtype=pa.string()),
            ArrowFieldSpec(name="raw_line", dtype=pa.int32()),
            ArrowFieldSpec(name="raw_column", dtype=pa.int32()),
        ],
    )
)

NAME_REFS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_name_refs_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="name_ref_id", dtype=pa.string()),
            ArrowFieldSpec(name="name", dtype=pa.string()),
            ArrowFieldSpec(name="expr_ctx", dtype=pa.string()),
            ArrowFieldSpec(name="bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="bend", dtype=pa.int64()),
        ],
    )
)

IMPORTS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_imports_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="import_id", dtype=pa.string()),
            ArrowFieldSpec(name="kind", dtype=pa.string()),
            ArrowFieldSpec(name="module", dtype=pa.string()),
            ArrowFieldSpec(name="relative_level", dtype=pa.int32()),
            ArrowFieldSpec(name="name", dtype=pa.string()),
            ArrowFieldSpec(name="asname", dtype=pa.string()),
            ArrowFieldSpec(name="is_star", dtype=pa.bool_()),
            ArrowFieldSpec(name="stmt_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="stmt_bend", dtype=pa.int64()),
            ArrowFieldSpec(name="alias_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="alias_bend", dtype=pa.int64()),
        ],
    )
)

CALLSITES_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_callsites_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="call_id", dtype=pa.string()),
            *call_span_bundle().fields,
            ArrowFieldSpec(name="callee_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="callee_bend", dtype=pa.int64()),
            ArrowFieldSpec(name="callee_shape", dtype=pa.string()),
            ArrowFieldSpec(name="callee_text", dtype=pa.string()),
            ArrowFieldSpec(name="arg_count", dtype=pa.int32()),
            ArrowFieldSpec(name="callee_dotted", dtype=pa.string()),
            ArrowFieldSpec(name="callee_qnames", dtype=QNAME_LIST),
        ],
    )
)

DEFS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_defs_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="def_id", dtype=pa.string()),
            ArrowFieldSpec(name="container_def_id", dtype=pa.string()),
            ArrowFieldSpec(name="kind", dtype=pa.string()),
            ArrowFieldSpec(name="name", dtype=pa.string()),
            ArrowFieldSpec(name="def_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="def_bend", dtype=pa.int64()),
            ArrowFieldSpec(name="name_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="name_bend", dtype=pa.int64()),
            ArrowFieldSpec(name="qnames", dtype=QNAME_LIST),
        ],
    )
)

TYPE_EXPRS_SPEC = GLOBAL_SCHEMA_REGISTRY.register_table(
    make_table_spec(
        name="py_cst_type_exprs_v1",
        version=SCHEMA_VERSION,
        bundles=(file_identity_bundle(),),
        fields=[
            ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
            ArrowFieldSpec(name="owner_def_id", dtype=pa.string()),
            ArrowFieldSpec(name="param_name", dtype=pa.string()),
            ArrowFieldSpec(name="expr_kind", dtype=pa.string()),
            ArrowFieldSpec(name="expr_role", dtype=pa.string()),
            ArrowFieldSpec(name="bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="bend", dtype=pa.int64()),
            ArrowFieldSpec(name="expr_text", dtype=pa.string()),
        ],
    )
)

PARSE_MANIFEST_SCHEMA = PARSE_MANIFEST_SPEC.to_arrow_schema()
PARSE_ERRORS_SCHEMA = PARSE_ERRORS_SPEC.to_arrow_schema()
NAME_REFS_SCHEMA = NAME_REFS_SPEC.to_arrow_schema()
IMPORTS_SCHEMA = IMPORTS_SPEC.to_arrow_schema()
CALLSITES_SCHEMA = CALLSITES_SPEC.to_arrow_schema()
DEFS_SCHEMA = DEFS_SPEC.to_arrow_schema()
TYPE_EXPRS_SCHEMA = TYPE_EXPRS_SPEC.to_arrow_schema()


@dataclass(frozen=True)
class CSTFileContext:
    """Per-file context for CST extraction."""

    file_ctx: FileContext
    options: CSTExtractOptions

    @property
    def file_id(self) -> str:
        """Return the file id for this extraction context.

        Returns
        -------
        str
            File id from the file context.
        """
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return the file path for this extraction context.

        Returns
        -------
        str
            File path from the file context.
        """
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return the file sha256 for this extraction context.

        Returns
        -------
        str | None
            File hash from the file context.
        """
        return self.file_ctx.file_sha256


@dataclass
class CSTExtractContext:
    """Shared extraction context and output buffers."""

    options: CSTExtractOptions
    manifest_rows: list[Row]
    manifest_future_imports_offsets: list[int]
    manifest_future_imports_values: list[str | None]
    error_rows: list[Row]
    name_ref_rows: list[Row]
    import_rows: list[Row]
    call_rows: list[Row]
    call_qname_offsets: list[int]
    call_qname_names: list[str]
    call_qname_sources: list[str]
    def_rows: list[Row]
    def_qname_offsets: list[int]
    def_qname_names: list[str]
    def_qname_sources: list[str]
    type_expr_rows: list[Row]

    @classmethod
    def build(cls, options: CSTExtractOptions) -> CSTExtractContext:
        """Create an empty extraction context.

        Returns
        -------
        CSTExtractContext
            New extraction context with empty buffers.
        """
        return cls(
            options=options,
            manifest_rows=[],
            manifest_future_imports_offsets=_offsets_start(),
            manifest_future_imports_values=[],
            error_rows=[],
            name_ref_rows=[],
            import_rows=[],
            call_rows=[],
            call_qname_offsets=_offsets_start(),
            call_qname_names=[],
            call_qname_sources=[],
            def_rows=[],
            def_qname_offsets=_offsets_start(),
            def_qname_names=[],
            def_qname_sources=[],
            type_expr_rows=[],
        )


type DefKey = tuple[str, int, int]


@dataclass(frozen=True)
class TypeExprOwner:
    """Context for a collected type expression."""

    owner_def_kind: str
    owner_def_bstart: int
    owner_def_bend: int
    expr_role: str
    param_name: str | None = None


def _iter_params(params: cst.Parameters) -> list[cst.Param]:
    items: list[cst.Param] = []
    items.extend(params.posonly_params)
    items.extend(params.params)
    items.extend(params.kwonly_params)
    if isinstance(params.star_arg, cst.Param):
        items.append(params.star_arg)
    if isinstance(params.star_kwarg, cst.Param):
        items.append(params.star_kwarg)
    return items


def _callee_shape(node: cst.CSTNode) -> str:
    if isinstance(node, cst.Name):
        return "NAME"
    if isinstance(node, cst.Attribute):
        return "ATTRIBUTE"
    if isinstance(node, cst.Subscript):
        return "SUBSCRIPT"
    return "OTHER"


def _calculate_module_and_package(repo_root: Path, abs_path: Path) -> tuple[str | None, str | None]:
    try:
        result = helpers.calculate_module_and_package(str(repo_root), str(abs_path))
    except (OSError, ValueError):
        return None, None
    else:
        return result.name, result.package


def _row_bytes(file_ctx: FileContext) -> bytes | None:
    if file_ctx.data is not None:
        return file_ctx.data
    if not file_ctx.text:
        return None
    encoding = file_ctx.encoding or "utf-8"
    return file_ctx.text.encode(encoding, errors="replace")


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
    *,
    abs_path: str | Path | None,
    options: CSTExtractOptions,
    path: str,
) -> tuple[str | None, str | None]:
    if options.repo_root is None:
        return None, None
    if isinstance(abs_path, Path):
        abs_path_value = abs_path
    elif isinstance(abs_path, str):
        abs_path_value = Path(abs_path)
    else:
        abs_path_value = Path(path)
    return _calculate_module_and_package(options.repo_root, abs_path_value)


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
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
        "encoding": getattr(module, "encoding", None),
        "default_indent": getattr(module, "default_indent", None),
        "default_newline": getattr(module, "default_newline", None),
        "has_trailing_newline": bool(getattr(module, "has_trailing_newline", False)),
        "future_imports": None,
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


@dataclass(frozen=True)
class CollectorContext:
    """Context required to build a CST collector."""

    module: cst.Module
    file_ctx: CSTFileContext
    extract_ctx: CSTExtractContext


@dataclass(frozen=True)
class CollectorMetadata:
    """Metadata maps needed by the CST collector."""

    span_map: Mapping[cst.CSTNode, object]
    ctx_map: Mapping[cst.CSTNode, object]
    qn_map: Mapping[cst.CSTNode, Collection[QualifiedName]]


def _visit_with_collector(
    module: cst.Module,
    *,
    file_ctx: CSTFileContext,
    ctx: CSTExtractContext,
) -> None:
    wrapper = MetadataWrapper(module, unsafe_skip_copy=True)
    span_map, ctx_map, qn_map = _resolve_metadata_maps(wrapper, ctx.options)
    collector = CSTCollector(
        ctx=CollectorContext(module=module, file_ctx=file_ctx, extract_ctx=ctx),
        meta=CollectorMetadata(span_map=span_map, ctx_map=ctx_map, qn_map=qn_map),
    )
    wrapper.visit(collector)


class CSTCollector(cst.CSTVisitor):
    """Collect CST-derived rows for a single file."""

    def __init__(
        self,
        *,
        ctx: CollectorContext,
        meta: CollectorMetadata,
    ) -> None:
        self._module = ctx.module
        self._file_ctx = ctx.file_ctx
        self._options = ctx.extract_ctx.options
        self._span_map = meta.span_map
        self._ctx_map = meta.ctx_map
        self._qn_map = meta.qn_map
        self._name_ref_rows = ctx.extract_ctx.name_ref_rows
        self._import_rows = ctx.extract_ctx.import_rows
        self._call_rows = ctx.extract_ctx.call_rows
        self._call_qname_offsets = ctx.extract_ctx.call_qname_offsets
        self._call_qname_names = ctx.extract_ctx.call_qname_names
        self._call_qname_sources = ctx.extract_ctx.call_qname_sources
        self._def_rows = ctx.extract_ctx.def_rows
        self._def_qname_offsets = ctx.extract_ctx.def_qname_offsets
        self._def_qname_names = ctx.extract_ctx.def_qname_names
        self._def_qname_sources = ctx.extract_ctx.def_qname_sources
        self._type_expr_rows = ctx.extract_ctx.type_expr_rows
        self._skip_name_nodes: set[cst.Name] = set()
        self._def_stack: list[DefKey] = []

    def _span(self, node: cst.CSTNode) -> tuple[int, int]:
        sp = self._span_map.get(node)
        if sp is None:
            return 0, 0
        start = int(getattr(sp, "start", 0))
        length = int(getattr(sp, "length", 0))
        return start, start + length

    def _code_for_node(self, node: cst.CSTNode) -> str | None:
        try:
            return self._module.code_for_node(node)
        except (AttributeError, ValueError):
            return None

    def _qnames(self, node: cst.CSTNode) -> list[tuple[str, str]]:
        qset = self._qn_map.get(node)
        if not qset:
            return []
        qualified = sorted(qset, key=lambda q: (q.name, str(q.source)))
        return [(q.name, str(q.source)) for q in qualified]

    def _record_type_expr(
        self,
        annotation: cst.Annotation,
        *,
        owner: TypeExprOwner,
    ) -> None:
        if not self._options.include_type_exprs:
            return
        expr = annotation.annotation
        bstart, bend = self._span(expr)
        expr_text = self._code_for_node(expr)
        if expr_text is None:
            return
        self._type_expr_rows.append(
            {
                "owner_def_kind": owner.owner_def_kind,
                "owner_def_bstart": owner.owner_def_bstart,
                "owner_def_bend": owner.owner_def_bend,
                "param_name": owner.param_name,
                "expr_kind": "annotation",
                "expr_role": owner.expr_role,
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "bstart": bstart,
                "bend": bend,
                "expr_text": expr_text,
            }
        )

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
        container_kind: str | None = None
        container_bstart: int | None = None
        container_bend: int | None = None
        if container is not None:
            container_kind, container_bstart, container_bend = container
        def_kind = "async_function" if node.asynchronous is not None else "function"
        def_key: DefKey = (def_kind, def_bstart, def_bend)
        qnames = self._qnames(node)
        _append_qname_list(
            self._def_qname_offsets,
            self._def_qname_names,
            self._def_qname_sources,
            qnames,
        )

        self._def_rows.append(
            {
                "container_def_kind": container_kind,
                "container_def_bstart": container_bstart,
                "container_def_bend": container_bend,
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "kind": def_kind,
                "name": node.name.value,
                "def_bstart": def_bstart,
                "def_bend": def_bend,
                "name_bstart": name_bstart,
                "name_bend": name_bend,
                "qnames": None,
            }
        )
        if node.returns is not None:
            self._record_type_expr(
                node.returns,
                owner=TypeExprOwner(
                    owner_def_kind=def_kind,
                    owner_def_bstart=def_bstart,
                    owner_def_bend=def_bend,
                    expr_role="return",
                ),
            )
        for param in _iter_params(node.params):
            if param.annotation is None:
                continue
            self._record_type_expr(
                param.annotation,
                owner=TypeExprOwner(
                    owner_def_kind=def_kind,
                    owner_def_bstart=def_bstart,
                    owner_def_bend=def_bend,
                    expr_role="param",
                    param_name=param.name.value,
                ),
            )
        self._def_stack.append(def_key)
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
        container_kind: str | None = None
        container_bstart: int | None = None
        container_bend: int | None = None
        if container is not None:
            container_kind, container_bstart, container_bend = container
        def_kind = "class"
        def_key: DefKey = (def_kind, def_bstart, def_bend)
        qnames = self._qnames(node)
        _append_qname_list(
            self._def_qname_offsets,
            self._def_qname_names,
            self._def_qname_sources,
            qnames,
        )

        self._def_rows.append(
            {
                "container_def_kind": container_kind,
                "container_def_bstart": container_bstart,
                "container_def_bend": container_bend,
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "kind": def_kind,
                "name": node.name.value,
                "def_bstart": def_bstart,
                "def_bend": def_bend,
                "name_bstart": name_bstart,
                "name_bend": name_bend,
                "qnames": None,
            }
        )
        self._def_stack.append(def_key)
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
        callee_shape = _callee_shape(node.func)
        callee_text = self._code_for_node(node.func)
        arg_count = len(node.args)

        qnames = self._qnames(node.func)
        if not qnames:
            qnames = self._qnames(node)
        _append_qname_list(
            self._call_qname_offsets,
            self._call_qname_names,
            self._call_qname_sources,
            qnames,
        )

        self._call_rows.append(
            {
                "file_id": self._file_ctx.file_id,
                "path": self._file_ctx.path,
                "file_sha256": self._file_ctx.file_sha256,
                "call_bstart": call_bstart,
                "call_bend": call_bend,
                "callee_bstart": callee_bstart,
                "callee_bend": callee_bend,
                "callee_shape": callee_shape,
                "callee_text": callee_text,
                "arg_count": int(arg_count),
                "callee_dotted": callee_dotted,
                "callee_qnames": None,
            }
        )
        return True


def _extract_cst_for_context(file_ctx: FileContext, ctx: CSTExtractContext) -> None:
    if not file_ctx.file_id or not file_ctx.path:
        return

    data = _row_bytes(file_ctx)
    if data is None:
        return

    module = _parse_or_record_error(
        data,
        file_id=file_ctx.file_id,
        path=file_ctx.path,
        file_sha256=file_ctx.file_sha256,
        ctx=ctx,
    )
    if module is None:
        return

    module_name, package_name = _module_package_names(
        abs_path=file_ctx.abs_path,
        options=ctx.options,
        path=file_ctx.path,
    )
    cst_file_ctx = CSTFileContext(file_ctx=file_ctx, options=ctx.options)
    if ctx.options.include_parse_manifest:
        future_imports = getattr(module, "future_imports", []) or []
        _append_string_list(
            ctx.manifest_future_imports_offsets,
            ctx.manifest_future_imports_values,
            future_imports,
        )
        ctx.manifest_rows.append(
            _manifest_row(
                cst_file_ctx,
                module,
                module_name=module_name,
                package_name=package_name,
            )
        )

    _visit_with_collector(module, file_ctx=cst_file_ctx, ctx=ctx)


def _extract_cst_for_row(rf: dict[str, object], ctx: CSTExtractContext) -> None:
    file_ctx = FileContext.from_repo_row(rf)
    _extract_cst_for_context(file_ctx, ctx)


def extract_cst(
    repo_files: TableLike,
    options: CSTExtractOptions | None = None,
    *,
    file_contexts: Iterable[FileContext] | None = None,
) -> CSTExtractResult:
    """Extract LibCST-derived structures from repo files.

    Returns
    -------
    CSTExtractResult
        Tables derived from LibCST parsing and metadata providers.
    """
    options = options or CSTExtractOptions()
    ctx = CSTExtractContext.build(options)

    contexts = file_contexts if file_contexts is not None else iter_file_contexts(repo_files)
    for file_ctx in contexts:
        _extract_cst_for_context(file_ctx, ctx)

    return _build_cst_result(ctx)


def _build_manifest_table(ctx: CSTExtractContext) -> TableLike:
    table = (
        pa.Table.from_pylist(ctx.manifest_rows, schema=PARSE_MANIFEST_SCHEMA)
        if ctx.manifest_rows
        else empty_table(PARSE_MANIFEST_SCHEMA)
    )
    if not ctx.manifest_rows:
        return table
    future_imports = build_list_array(
        pa.array(ctx.manifest_future_imports_offsets, type=pa.int32()),
        pa.array(ctx.manifest_future_imports_values, type=pa.string()),
    )
    idx = table.schema.get_field_index("future_imports")
    return table.set_column(idx, "future_imports", future_imports)


def _build_errors_table(ctx: CSTExtractContext) -> TableLike:
    return (
        pa.Table.from_pylist(ctx.error_rows, schema=PARSE_ERRORS_SCHEMA)
        if ctx.error_rows
        else empty_table(PARSE_ERRORS_SCHEMA)
    )


def _build_name_refs_table(ctx: CSTExtractContext) -> TableLike:
    if not ctx.name_ref_rows:
        return empty_table(NAME_REFS_SCHEMA)
    table = pa.Table.from_pylist(ctx.name_ref_rows)
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_name_ref",
            cols=("file_id", "bstart", "bend"),
            out_col="name_ref_id",
        ),
        required=("file_id", "bstart", "bend"),
    )
    return SchemaTransform(schema=NAME_REFS_SCHEMA).apply(table)


def _build_imports_table(ctx: CSTExtractContext) -> TableLike:
    if not ctx.import_rows:
        return empty_table(IMPORTS_SCHEMA)
    table = pa.Table.from_pylist(ctx.import_rows)
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_import",
            cols=("file_id", "kind", "alias_bstart", "alias_bend"),
            out_col="import_id",
        ),
        required=("file_id", "kind", "alias_bstart", "alias_bend"),
    )
    return SchemaTransform(schema=IMPORTS_SCHEMA).apply(table)


def _build_callsites_table(ctx: CSTExtractContext) -> TableLike:
    if not ctx.call_rows:
        return empty_table(CALLSITES_SCHEMA)
    table = pa.Table.from_pylist(ctx.call_rows)
    call_qnames = build_list_of_structs(
        pa.array(ctx.call_qname_offsets, type=pa.int32()),
        {
            "name": pa.array(ctx.call_qname_names, type=pa.string()),
            "source": pa.array(ctx.call_qname_sources, type=pa.string()),
        },
    )
    idx = table.schema.get_field_index("callee_qnames")
    table = table.set_column(idx, "callee_qnames", call_qnames)
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_call",
            cols=("file_id", "call_bstart", "call_bend"),
            out_col="call_id",
        ),
        required=("file_id", "call_bstart", "call_bend"),
    )
    return SchemaTransform(schema=CALLSITES_SCHEMA).apply(table)


def _build_defs_table(ctx: CSTExtractContext) -> TableLike:
    if not ctx.def_rows:
        return empty_table(DEFS_SCHEMA)
    table = pa.Table.from_pylist(ctx.def_rows)
    def_qnames = build_list_of_structs(
        pa.array(ctx.def_qname_offsets, type=pa.int32()),
        {
            "name": pa.array(ctx.def_qname_names, type=pa.string()),
            "source": pa.array(ctx.def_qname_sources, type=pa.string()),
        },
    )
    idx = table.schema.get_field_index("qnames")
    table = table.set_column(idx, "qnames", def_qnames)
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_def",
            cols=("file_id", "kind", "def_bstart", "def_bend"),
            out_col="def_id",
        ),
        required=("file_id", "kind", "def_bstart", "def_bend"),
    )
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_def",
            cols=("file_id", "container_def_kind", "container_def_bstart", "container_def_bend"),
            out_col="container_def_id",
        ),
        required=("file_id", "container_def_kind", "container_def_bstart", "container_def_bend"),
    )
    return SchemaTransform(schema=DEFS_SCHEMA).apply(table)


def _build_type_exprs_table(ctx: CSTExtractContext) -> TableLike:
    if not ctx.type_expr_rows:
        return empty_table(TYPE_EXPRS_SCHEMA)
    table = pa.Table.from_pylist(ctx.type_expr_rows)
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_type_expr",
            cols=("path", "bstart", "bend"),
            out_col="type_expr_id",
        ),
        required=("path", "bstart", "bend"),
    )
    table = _apply_hash_column(
        table,
        spec=HashSpec(
            prefix="cst_def",
            cols=("file_id", "owner_def_kind", "owner_def_bstart", "owner_def_bend"),
            out_col="owner_def_id",
        ),
        required=("file_id", "owner_def_kind", "owner_def_bstart", "owner_def_bend"),
    )
    return SchemaTransform(schema=TYPE_EXPRS_SCHEMA).apply(table)


def _build_cst_result(ctx: CSTExtractContext) -> CSTExtractResult:
    return CSTExtractResult(
        py_cst_parse_manifest=_build_manifest_table(ctx),
        py_cst_parse_errors=_build_errors_table(ctx),
        py_cst_name_refs=_build_name_refs_table(ctx),
        py_cst_imports=_build_imports_table(ctx),
        py_cst_callsites=_build_callsites_table(ctx),
        py_cst_defs=_build_defs_table(ctx),
        py_cst_type_exprs=_build_type_exprs_table(ctx),
    )


def extract_cst_tables(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
    ctx: object | None = None,
) -> dict[str, TableLike]:
    """Extract CST tables as a name-keyed bundle.

    Parameters
    ----------
    repo_root:
        Optional repository root for module/package derivation.
    repo_files:
        Repo files table.
    file_contexts:
        Optional pre-built file contexts for extraction.
    ctx:
        Execution context (unused).

    Returns
    -------
    dict[str, pyarrow.Table]
        Extracted CST tables keyed by output name.
    """
    _ = ctx
    options = CSTExtractOptions(repo_root=Path(repo_root) if repo_root else None)
    result = extract_cst(repo_files, options=options, file_contexts=file_contexts)
    return {
        "cst_parse_manifest": result.py_cst_parse_manifest,
        "cst_parse_errors": result.py_cst_parse_errors,
        "cst_name_refs": result.py_cst_name_refs,
        "cst_imports": result.py_cst_imports,
        "cst_callsites": result.py_cst_callsites,
        "cst_defs": result.py_cst_defs,
        "cst_type_exprs": result.py_cst_type_exprs,
    }
