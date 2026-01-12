"""Extract Python symtable data into Arrow tables."""

from __future__ import annotations

import symtable
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

import arrowdsl.pyarrow_core as pa
from arrowdsl.ids import prefixed_hash_id_from_parts
from arrowdsl.iter import iter_table_rows
from arrowdsl.pyarrow_protocols import TableLike
from extract.file_context import FileContext
from schema_spec.core import ArrowFieldSpec
from schema_spec.factories import make_table_spec
from schema_spec.fields import file_identity_bundle

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SymtableExtractOptions:
    """Configure symtable extraction."""

    compile_type: str = "exec"


@dataclass(frozen=True)
class SymtableExtractResult:
    """Hold extracted symtable tables for scopes, symbols, and edges."""

    py_sym_scopes: TableLike
    py_sym_symbols: TableLike
    py_sym_scope_edges: TableLike
    py_sym_namespace_edges: TableLike
    py_sym_function_partitions: TableLike


@dataclass(frozen=True)
class SymtableContext:
    """Context values shared across symtable rows."""

    file_ctx: FileContext

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


SCOPES_SPEC = make_table_spec(
    name="py_sym_scopes_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="table_id", dtype=pa.int64()),
        ArrowFieldSpec(name="scope_type", dtype=pa.string()),
        ArrowFieldSpec(name="scope_name", dtype=pa.string()),
        ArrowFieldSpec(name="lineno", dtype=pa.int32()),
        ArrowFieldSpec(name="is_nested", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_optimized", dtype=pa.bool_()),
        ArrowFieldSpec(name="has_children", dtype=pa.bool_()),
        ArrowFieldSpec(name="scope_role", dtype=pa.string()),
    ],
)

SYMBOLS_SPEC = make_table_spec(
    name="py_sym_symbols_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="symbol_row_id", dtype=pa.string()),
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="name", dtype=pa.string()),
        ArrowFieldSpec(name="is_referenced", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_assigned", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_imported", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_annotated", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_parameter", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_global", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_declared_global", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_nonlocal", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_local", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_free", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_namespace", dtype=pa.bool_()),
    ],
)

SCOPE_EDGES_SPEC = make_table_spec(
    name="py_sym_scope_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="parent_scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="child_scope_id", dtype=pa.string()),
    ],
)

NAMESPACE_EDGES_SPEC = make_table_spec(
    name="py_sym_namespace_edges_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="edge_id", dtype=pa.string()),
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="symbol_row_id", dtype=pa.string()),
        ArrowFieldSpec(name="child_scope_id", dtype=pa.string()),
    ],
)

FUNC_PARTS_SPEC = make_table_spec(
    name="py_sym_function_partitions_v1",
    version=SCHEMA_VERSION,
    bundles=(file_identity_bundle(),),
    fields=[
        ArrowFieldSpec(name="scope_id", dtype=pa.string()),
        ArrowFieldSpec(name="parameters", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="locals", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="globals", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="nonlocals", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="frees", dtype=pa.list_(pa.string())),
    ],
)

SCOPES_SCHEMA = SCOPES_SPEC.to_arrow_schema()
SYMBOLS_SCHEMA = SYMBOLS_SPEC.to_arrow_schema()
SCOPE_EDGES_SCHEMA = SCOPE_EDGES_SPEC.to_arrow_schema()
NAMESPACE_EDGES_SCHEMA = NAMESPACE_EDGES_SPEC.to_arrow_schema()
FUNC_PARTS_SCHEMA = FUNC_PARTS_SPEC.to_arrow_schema()


def _scope_type_str(tbl: symtable.SymbolTable) -> str:
    return str(tbl.get_type()).split(".")[-1]


def _scope_role(scope_type_str: str) -> str:
    return "runtime" if scope_type_str in {"MODULE", "FUNCTION", "CLASS"} else "type_meta"


def _ensure_scope_id(
    tbl: symtable.SymbolTable,
    *,
    ctx: SymtableContext,
    table_to_scope_id: dict[int, str],
) -> str:
    tid = int(tbl.get_id())
    sid = table_to_scope_id.get(tid)
    if sid is None:
        st_str = _scope_type_str(tbl)
        name = tbl.get_name()
        lineno = int(tbl.get_lineno() or 0)
        sid = prefixed_hash_id_from_parts(
            "sym_scope",
            ctx.file_id,
            str(tid),
            st_str,
            name,
            str(lineno),
        )
        table_to_scope_id[tid] = sid
    return sid


def _scope_row(ctx: SymtableContext, sid: str, tbl: symtable.SymbolTable) -> dict[str, object]:
    st_str = _scope_type_str(tbl)
    return {
        "scope_id": sid,
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
        "table_id": int(tbl.get_id()),
        "scope_type": st_str,
        "scope_name": tbl.get_name(),
        "lineno": int(tbl.get_lineno() or 0),
        "is_nested": bool(tbl.is_nested()),
        "is_optimized": bool(tbl.is_optimized()),
        "has_children": bool(tbl.has_children()),
        "scope_role": _scope_role(st_str),
    }


def _scope_edge_row(ctx: SymtableContext, parent_sid: str, child_sid: str) -> dict[str, object]:
    return {
        "edge_id": prefixed_hash_id_from_parts("sym_scope_edge", parent_sid, child_sid),
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
        "parent_scope_id": parent_sid,
        "child_scope_id": child_sid,
    }


def _symbol_rows_for_scope(
    ctx: SymtableContext,
    *,
    scope_id: str,
    tbl: symtable.SymbolTable,
    table_to_scope_id: dict[int, str],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    symbol_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []

    for sym in tbl.get_symbols():
        name = sym.get_name()
        sym_row_id = prefixed_hash_id_from_parts("sym_symbol", scope_id, name)
        symbol_rows.append(
            {
                "symbol_row_id": sym_row_id,
                "scope_id": scope_id,
                "file_id": ctx.file_id,
                "path": ctx.path,
                "file_sha256": ctx.file_sha256,
                "name": name,
                "is_referenced": bool(sym.is_referenced()),
                "is_assigned": bool(sym.is_assigned()),
                "is_imported": bool(sym.is_imported()),
                "is_annotated": bool(sym.is_annotated()),
                "is_parameter": bool(sym.is_parameter()),
                "is_global": bool(sym.is_global()),
                "is_declared_global": bool(sym.is_declared_global()),
                "is_nonlocal": bool(sym.is_nonlocal()),
                "is_local": bool(sym.is_local()),
                "is_free": bool(sym.is_free()),
                "is_namespace": bool(sym.is_namespace()),
            }
        )

        if bool(sym.is_namespace()):
            for nt in _iter_namespaces(sym):
                child_sid = _ensure_scope_id(nt, ctx=ctx, table_to_scope_id=table_to_scope_id)
                ns_edge_rows.append(
                    {
                        "edge_id": prefixed_hash_id_from_parts(
                            "sym_ns_edge",
                            sym_row_id,
                            child_sid,
                        ),
                        "file_id": ctx.file_id,
                        "path": ctx.path,
                        "file_sha256": ctx.file_sha256,
                        "scope_id": scope_id,
                        "symbol_row_id": sym_row_id,
                        "child_scope_id": child_sid,
                    }
                )

    return symbol_rows, ns_edge_rows


def _func_parts_row(
    ctx: SymtableContext, scope_id: str, tbl: symtable.SymbolTable
) -> dict[str, object] | None:
    if _scope_type_str(tbl) != "FUNCTION":
        return None
    return {
        "scope_id": scope_id,
        "file_id": ctx.file_id,
        "path": ctx.path,
        "file_sha256": ctx.file_sha256,
        "parameters": _symbol_list(tbl, "get_parameters"),
        "locals": _symbol_list(tbl, "get_locals"),
        "globals": _symbol_list(tbl, "get_globals"),
        "nonlocals": _symbol_list(tbl, "get_nonlocals"),
        "frees": _symbol_list(tbl, "get_frees"),
    }


def _repo_text(file_ctx: FileContext) -> str | None:
    if file_ctx.text:
        return file_ctx.text
    if file_ctx.data is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    try:
        return file_ctx.data.decode(encoding, errors="replace")
    except UnicodeError:
        return None


def _extract_symtable_for_row(
    row: dict[str, object],
    *,
    compile_type: str,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    file_ctx = FileContext.from_repo_row(row)
    if not file_ctx.file_id or not file_ctx.path:
        return [], [], [], [], []

    text = _repo_text(file_ctx)
    if not text:
        return [], [], [], [], []

    try:
        top = symtable.symtable(text, file_ctx.path, compile_type)
    except (SyntaxError, TypeError, ValueError):
        return [], [], [], [], []

    ctx = SymtableContext(file_ctx=file_ctx)
    return _walk_symtable(top, ctx)


def _walk_symtable(
    top: symtable.SymbolTable,
    ctx: SymtableContext,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts_rows: list[dict[str, object]] = []
    table_to_scope_id: dict[int, str] = {}

    stack: list[tuple[symtable.SymbolTable, symtable.SymbolTable | None]] = [(top, None)]
    while stack:
        tbl, parent_tbl = stack.pop()
        sid = _ensure_scope_id(tbl, ctx=ctx, table_to_scope_id=table_to_scope_id)
        scope_rows.append(_scope_row(ctx, sid, tbl))

        if parent_tbl is not None:
            psid = _ensure_scope_id(parent_tbl, ctx=ctx, table_to_scope_id=table_to_scope_id)
            scope_edge_rows.append(_scope_edge_row(ctx, psid, sid))

        sym_rows, ns_rows = _symbol_rows_for_scope(
            ctx,
            scope_id=sid,
            tbl=tbl,
            table_to_scope_id=table_to_scope_id,
        )
        symbol_rows.extend(sym_rows)
        ns_edge_rows.extend(ns_rows)

        parts_row = _func_parts_row(ctx, sid, tbl)
        if parts_row is not None:
            func_parts_rows.append(parts_row)

        stack.extend((child, tbl) for child in _iter_children(tbl))

    return scope_rows, symbol_rows, scope_edge_rows, ns_edge_rows, func_parts_rows


def _symbol_list(tbl: symtable.SymbolTable, name: str) -> list[str]:
    fn = getattr(tbl, name, None)
    if callable(fn):
        values = cast("Sequence[str]", fn())
        return list(values)
    return []


def _iter_children(tbl: symtable.SymbolTable) -> list[symtable.SymbolTable]:
    return list(tbl.get_children())


def _iter_namespaces(sym: symtable.Symbol) -> list[symtable.SymbolTable]:
    return list(sym.get_namespaces())


def extract_symtable(
    repo_files: TableLike, options: SymtableExtractOptions | None = None
) -> SymtableExtractResult:
    """Extract symbol table artifacts from repository files.

    Returns
    -------
    SymtableExtractResult
        Tables for scopes, symbols, and namespace edges.
    """
    options = options or SymtableExtractOptions()

    scope_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    scope_edge_rows: list[dict[str, object]] = []
    ns_edge_rows: list[dict[str, object]] = []
    func_parts_rows: list[dict[str, object]] = []

    for rf in iter_table_rows(repo_files):
        (
            file_scope_rows,
            file_symbol_rows,
            file_scope_edge_rows,
            file_ns_edge_rows,
            file_func_parts_rows,
        ) = _extract_symtable_for_row(rf, compile_type=options.compile_type)
        scope_rows.extend(file_scope_rows)
        symbol_rows.extend(file_symbol_rows)
        scope_edge_rows.extend(file_scope_edge_rows)
        ns_edge_rows.extend(file_ns_edge_rows)
        func_parts_rows.extend(file_func_parts_rows)

    return SymtableExtractResult(
        py_sym_scopes=pa.Table.from_pylist(scope_rows, schema=SCOPES_SCHEMA),
        py_sym_symbols=pa.Table.from_pylist(symbol_rows, schema=SYMBOLS_SCHEMA),
        py_sym_scope_edges=pa.Table.from_pylist(scope_edge_rows, schema=SCOPE_EDGES_SCHEMA),
        py_sym_namespace_edges=pa.Table.from_pylist(ns_edge_rows, schema=NAMESPACE_EDGES_SCHEMA),
        py_sym_function_partitions=pa.Table.from_pylist(func_parts_rows, schema=FUNC_PARTS_SCHEMA),
    )


def extract_symtables_table(
    *,
    repo_root: str | None,
    repo_files: TableLike,
    ctx: object | None = None,
) -> TableLike:
    """Extract symtable data into a single table.

    Parameters
    ----------
    repo_root:
        Optional repository root (unused).
    repo_files:
        Repo files table.
    ctx:
        Execution context (unused).

    Returns
    -------
    pyarrow.Table
        Symtable extraction table.
    """
    _ = (repo_root, ctx)
    result = extract_symtable(repo_files)
    return result.py_sym_scopes
