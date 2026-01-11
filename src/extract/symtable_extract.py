"""Extract Python symtable data into Arrow tables."""

from __future__ import annotations

import symtable
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa

from extract.repo_scan import stable_id

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SymtableExtractOptions:
    """Configure symtable extraction."""

    compile_type: str = "exec"


@dataclass(frozen=True)
class SymtableExtractResult:
    """Hold extracted symtable tables for scopes, symbols, and edges."""

    py_sym_scopes: pa.Table
    py_sym_symbols: pa.Table
    py_sym_scope_edges: pa.Table
    py_sym_namespace_edges: pa.Table
    py_sym_function_partitions: pa.Table


@dataclass(frozen=True)
class SymtableContext:
    """Context values shared across symtable rows."""

    file_id: str
    path: str
    file_sha256: str | None


SCOPES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("scope_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("table_id", pa.int64()),
        ("scope_type", pa.string()),
        ("scope_name", pa.string()),
        ("lineno", pa.int32()),
        ("is_nested", pa.bool_()),
        ("is_optimized", pa.bool_()),
        ("has_children", pa.bool_()),
        ("scope_role", pa.string()),
    ]
)

SYMBOLS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("symbol_row_id", pa.string()),
        ("scope_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("name", pa.string()),
        ("is_referenced", pa.bool_()),
        ("is_assigned", pa.bool_()),
        ("is_imported", pa.bool_()),
        ("is_annotated", pa.bool_()),
        ("is_parameter", pa.bool_()),
        ("is_global", pa.bool_()),
        ("is_declared_global", pa.bool_()),
        ("is_nonlocal", pa.bool_()),
        ("is_local", pa.bool_()),
        ("is_free", pa.bool_()),
        ("is_namespace", pa.bool_()),
    ]
)

SCOPE_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("parent_scope_id", pa.string()),
        ("child_scope_id", pa.string()),
    ]
)

NAMESPACE_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("scope_id", pa.string()),
        ("symbol_row_id", pa.string()),
        ("child_scope_id", pa.string()),
    ]
)

FUNC_PARTS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("scope_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("parameters", pa.list_(pa.string())),
        ("locals", pa.list_(pa.string())),
        ("globals", pa.list_(pa.string())),
        ("nonlocals", pa.list_(pa.string())),
        ("frees", pa.list_(pa.string())),
    ]
)


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
        sid = stable_id("sym_scope", ctx.file_id, str(tid), st_str, name, str(lineno))
        table_to_scope_id[tid] = sid
    return sid


def _scope_row(ctx: SymtableContext, sid: str, tbl: symtable.SymbolTable) -> dict[str, object]:
    st_str = _scope_type_str(tbl)
    return {
        "schema_version": SCHEMA_VERSION,
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
        "schema_version": SCHEMA_VERSION,
        "edge_id": stable_id("sym_scope_edge", parent_sid, child_sid),
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
        sym_row_id = stable_id("sym_symbol", scope_id, name)
        symbol_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
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
                        "schema_version": SCHEMA_VERSION,
                        "edge_id": stable_id("sym_ns_edge", sym_row_id, child_sid),
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
        "schema_version": SCHEMA_VERSION,
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


def _repo_text(row: dict[str, object]) -> str | None:
    text = row.get("text")
    if isinstance(text, str) and text:
        return text
    raw = row.get("bytes")
    if isinstance(raw, (bytes, bytearray)):
        encoding_value = row.get("encoding")
        encoding = encoding_value if isinstance(encoding_value, str) else "utf-8"
        try:
            return bytes(raw).decode(encoding, errors="replace")
        except UnicodeError:
            return None
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
    file_id = row.get("file_id")
    path = row.get("path")
    if file_id is None or path is None:
        return [], [], [], [], []
    file_sha256_value = row.get("file_sha256")
    file_sha256 = file_sha256_value if isinstance(file_sha256_value, str) else None

    text = _repo_text(row)
    if not text:
        return [], [], [], [], []

    try:
        top = symtable.symtable(text, str(path), compile_type)
    except (SyntaxError, TypeError, ValueError):
        return [], [], [], [], []

    ctx = SymtableContext(file_id=str(file_id), path=str(path), file_sha256=file_sha256)
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
    repo_files: pa.Table, options: SymtableExtractOptions | None = None
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

    for rf in repo_files.to_pylist():
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
    repo_files: pa.Table,
    ctx: object | None = None,
) -> pa.Table:
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
