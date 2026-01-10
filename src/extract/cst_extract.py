"""Extract LibCST-derived structures into Arrow tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from .repo_scan import stable_id

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CSTExtractOptions:
    """
    LibCST extraction options.

    - bytes-first parse is always used when file bytes are available.
    - repo_root is optional; when set we compute (module, package) using libcst.helpers.
    """

    repo_id: str | None = None
    repo_root: Path | None = None

    include_parse_manifest: bool = True
    include_parse_errors: bool = True
    include_name_refs: bool = True
    include_imports: bool = True
    include_callsites: bool = True
    include_defs: bool = True

    # metadata providers:
    compute_expr_context: bool = True
    compute_qualified_names: bool = True


@dataclass(frozen=True)
class CSTExtractResult:
    """Extracted CST tables for manifests, errors, names, imports, and calls."""

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
        ("kind", pa.string()),  # "import" | "importfrom"
        ("module", pa.string()),  # module being imported from (nullable for "import")
        ("relative_level", pa.int32()),
        ("name", pa.string()),  # imported name (or module for "import")
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
        ("kind", pa.string()),  # "class" | "function" | "async_function"
        ("name", pa.string()),
        ("def_bstart", pa.int64()),
        ("def_bend", pa.int64()),
        ("name_bstart", pa.int64()),
        ("name_bend", pa.int64()),
        ("qnames", QNAME_LIST),
    ]
)


def _empty(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)


def extract_cst(repo_files: pa.Table, options: CSTExtractOptions | None = None) -> CSTExtractResult:
    """
    LibCST extraction.

    Important: this uses the bytes-first parse pattern:
      module = cst.parse_module(bytes)
      wrapper = MetadataWrapper(module, unsafe_skip_copy=True)

    This preserves module.encoding/default_newline/default_indent and supports byte spans
    via ByteSpanPositionProvider. :contentReference[oaicite:8]{index=8}:contentReference[oaicite:9]{index=9}

    Returns
    -------
    CSTExtractResult
        Tables derived from LibCST parsing and metadata providers.

    Raises
    ------
    RuntimeError
        Raised when LibCST cannot be imported.
    """
    options = options or CSTExtractOptions()

    try:
        import libcst as cst
        from libcst import helpers
        from libcst.metadata import (
            ByteSpanPositionProvider,
            ExpressionContextProvider,
            MetadataWrapper,
            QualifiedNameProvider,
        )
    except Exception as e:
        raise RuntimeError(
            "LibCST is required for cst_extract.py. Install libcst and rerun."
        ) from e

    manifest_rows: list[dict] = []
    error_rows: list[dict] = []
    name_ref_rows: list[dict] = []
    import_rows: list[dict] = []
    call_rows: list[dict] = []
    def_rows: list[dict] = []

    for rf in repo_files.to_pylist():
        file_id = rf["file_id"]
        path = rf["path"]
        file_sha256 = rf.get("file_sha256")
        data: bytes | None = rf.get("bytes")

        if data is None:
            # fallback: parse from text (less faithful)
            t = rf.get("text")
            if not t:
                continue
            data = t.encode(rf.get("encoding") or "utf-8", errors="replace")

        # Best-effort module/package derivation (optional)
        module_name: str | None = None
        package_name: str | None = None
        if options.repo_root is not None:
            try:
                abs_path = Path(rf["abs_path"])
                module_name, package_name = helpers.calculate_module_and_package(
                    str(options.repo_root), str(abs_path)
                )
            except Exception:
                module_name, package_name = None, None

        # Parse bytes-first
        try:
            module = cst.parse_module(data)
        except Exception as e:
            if options.include_parse_errors:
                error_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "error_type": type(e).__name__,
                        "message": str(e),
                        "raw_line": int(getattr(e, "raw_line", 0) or 0),
                        "raw_column": int(getattr(e, "raw_column", 0) or 0),
                    }
                )
            continue

        if options.include_parse_manifest:
            try:
                manifest_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "encoding": getattr(module, "encoding", None),
                        "default_indent": getattr(module, "default_indent", None),
                        "default_newline": getattr(module, "default_newline", None),
                        "has_trailing_newline": bool(
                            getattr(module, "has_trailing_newline", False)
                        ),
                        "future_imports": list(getattr(module, "future_imports", []) or []),
                        "module_name": module_name,
                        "package_name": package_name,
                    }
                )
            except Exception:
                # manifest is optional; ignore failures
                pass

        wrapper = MetadataWrapper(module, unsafe_skip_copy=True)

        providers: set[Any] = {ByteSpanPositionProvider}
        if options.compute_expr_context and (options.include_name_refs):
            providers.add(ExpressionContextProvider)
        if options.compute_qualified_names and (options.include_callsites or options.include_defs):
            providers.add(QualifiedNameProvider)

        resolved = wrapper.resolve_many(providers)
        span_map = resolved.get(ByteSpanPositionProvider, {})
        ctx_map = (
            resolved.get(ExpressionContextProvider, {})
            if ExpressionContextProvider in providers
            else {}
        )
        qn_map = (
            resolved.get(QualifiedNameProvider, {}) if QualifiedNameProvider in providers else {}
        )

        # Nodes to skip as "name refs" (def/class names)
        skip_name_nodes: set[Any] = set()

        def_stack: list[str] = []

        def _span(node: Any) -> tuple[int, int]:
            sp = span_map.get(node)
            if sp is None:
                return (0, 0)
            start = int(getattr(sp, "start", 0))
            length = int(getattr(sp, "length", 0))
            return (start, start + length)

        def _qnames(node: Any) -> list[dict]:
            qset = qn_map.get(node)
            if not qset:
                return []
            out = []
            for q in sorted(
                qset, key=lambda x: (getattr(x, "name", ""), str(getattr(x, "source", "")))
            ):
                out.append(
                    {"name": getattr(q, "name", None), "source": str(getattr(q, "source", ""))}
                )
            return out

        class Collector(cst.CSTVisitor):
            def visit_FunctionDef(self, node: cst.FunctionDef) -> bool | None:
                if not options.include_defs:
                    return True
                def_bstart, def_bend = _span(node)
                name_bstart, name_bend = _span(node.name)
                skip_name_nodes.add(node.name)

                container = def_stack[-1] if def_stack else None
                def_kind = "async_function" if node.asynchronous is not None else "function"
                def_id = stable_id("cst_def", file_id, def_kind, str(def_bstart), str(def_bend))

                def_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "def_id": def_id,
                        "container_def_id": container,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "kind": def_kind,
                        "name": node.name.value,
                        "def_bstart": def_bstart,
                        "def_bend": def_bend,
                        "name_bstart": name_bstart,
                        "name_bend": name_bend,
                        "qnames": _qnames(node),
                    }
                )
                def_stack.append(def_id)
                return True

            def leave_FunctionDef(self, original_node: cst.FunctionDef) -> None:
                if options.include_defs and def_stack:
                    def_stack.pop()

            def visit_ClassDef(self, node: cst.ClassDef) -> bool | None:
                if not options.include_defs:
                    return True
                def_bstart, def_bend = _span(node)
                name_bstart, name_bend = _span(node.name)
                skip_name_nodes.add(node.name)

                container = def_stack[-1] if def_stack else None
                def_kind = "class"
                def_id = stable_id("cst_def", file_id, def_kind, str(def_bstart), str(def_bend))

                def_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "def_id": def_id,
                        "container_def_id": container,
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "kind": def_kind,
                        "name": node.name.value,
                        "def_bstart": def_bstart,
                        "def_bend": def_bend,
                        "name_bstart": name_bstart,
                        "name_bend": name_bend,
                        "qnames": _qnames(node),
                    }
                )
                def_stack.append(def_id)
                return True

            def leave_ClassDef(self, original_node: cst.ClassDef) -> None:
                if options.include_defs and def_stack:
                    def_stack.pop()

            def visit_Name(self, node: cst.Name) -> bool | None:
                if not options.include_name_refs:
                    return True
                if node in skip_name_nodes:
                    return True
                bstart, bend = _span(node)
                expr_ctx = None
                if options.compute_expr_context:
                    ctx = ctx_map.get(node)
                    expr_ctx = str(ctx) if ctx is not None else None

                name_ref_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "name_ref_id": stable_id("cst_name_ref", file_id, str(bstart), str(bend)),
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "name": node.value,
                        "expr_ctx": expr_ctx,
                        "bstart": bstart,
                        "bend": bend,
                    }
                )
                return True

            def visit_Import(self, node: cst.Import) -> bool | None:
                if not options.include_imports:
                    return True
                stmt_bstart, stmt_bend = _span(node)
                for alias in node.names:
                    alias_bstart, alias_bend = _span(alias)
                    name = helpers.get_full_name_for_node(alias.name) or getattr(
                        alias.name, "value", None
                    )
                    asname = None
                    if alias.asname is not None:
                        try:
                            asname = alias.asname.name.value
                        except Exception:
                            asname = None

                    import_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "import_id": stable_id(
                                "cst_import", file_id, "import", str(alias_bstart), str(alias_bend)
                            ),
                            "file_id": file_id,
                            "path": path,
                            "file_sha256": file_sha256,
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
                if not options.include_imports:
                    return True
                stmt_bstart, stmt_bend = _span(node)

                module = None
                if node.module is not None:
                    module = helpers.get_full_name_for_node(node.module) or getattr(
                        node.module, "value", None
                    )

                # relative is a sequence of Dot nodes; count leading dots
                relative_level = 0
                try:
                    relative_level = len(node.relative) if node.relative is not None else 0
                except Exception:
                    relative_level = 0

                if isinstance(node.names, cst.ImportStar):
                    alias_bstart, alias_bend = _span(node.names)
                    import_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "import_id": stable_id(
                                "cst_import",
                                file_id,
                                "importfrom",
                                str(alias_bstart),
                                str(alias_bend),
                            ),
                            "file_id": file_id,
                            "path": path,
                            "file_sha256": file_sha256,
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
                    alias_bstart, alias_bend = _span(alias)
                    name = helpers.get_full_name_for_node(alias.name) or getattr(
                        alias.name, "value", None
                    )
                    asname = None
                    if alias.asname is not None:
                        try:
                            asname = alias.asname.name.value
                        except Exception:
                            asname = None

                    import_rows.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "import_id": stable_id(
                                "cst_import",
                                file_id,
                                "importfrom",
                                str(alias_bstart),
                                str(alias_bend),
                            ),
                            "file_id": file_id,
                            "path": path,
                            "file_sha256": file_sha256,
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
                if not options.include_callsites:
                    return True
                call_bstart, call_bend = _span(node)
                callee_bstart, callee_bend = _span(node.func)

                callee_dotted = None
                try:
                    callee_dotted = helpers.get_full_name_for_node(node.func)
                except Exception:
                    callee_dotted = None

                # Prefer qnames for node.func, but fall back to node if needed.
                qnames = _qnames(node.func)
                if not qnames:
                    qnames = _qnames(node)

                call_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "call_id": stable_id("cst_call", file_id, str(call_bstart), str(call_bend)),
                        "file_id": file_id,
                        "path": path,
                        "file_sha256": file_sha256,
                        "call_bstart": call_bstart,
                        "call_bend": call_bend,
                        "callee_bstart": callee_bstart,
                        "callee_bend": callee_bend,
                        "callee_dotted": callee_dotted,
                        "callee_qnames": qnames,
                    }
                )
                return True

        wrapper.visit(Collector())

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
