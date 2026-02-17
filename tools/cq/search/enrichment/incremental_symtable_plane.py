"""Incremental symtable enrichment plane."""

from __future__ import annotations

import symtable
from collections.abc import Iterable


def _bool_symbol_attr(symbol: symtable.Symbol, attr_name: str) -> bool:
    attr = getattr(symbol, attr_name, None)
    if not callable(attr):
        return False
    try:
        return bool(attr())
    except (RuntimeError, TypeError, ValueError):
        return False


def _safe_table_lineno(table: symtable.SymbolTable) -> int:
    getter = getattr(table, "get_lineno", None)
    if not callable(getter):
        return 0
    try:
        line = getter()
    except (RuntimeError, TypeError, ValueError):
        return 0
    if isinstance(line, int):
        return max(0, line)
    return 0


def _safe_table_type(table: symtable.SymbolTable) -> str:
    try:
        return str(table.get_type()).lower()
    except (RuntimeError, TypeError, ValueError):
        return "module"


def _safe_is_nested(table: symtable.SymbolTable) -> bool:
    checker = getattr(table, "is_nested", None)
    if not callable(checker):
        return False
    try:
        return bool(checker())
    except (RuntimeError, TypeError, ValueError):
        return False


def _safe_is_optimized(table: symtable.SymbolTable) -> bool:
    checker = getattr(table, "is_optimized", None)
    if not callable(checker):
        return False
    try:
        return bool(checker())
    except (RuntimeError, TypeError, ValueError):
        return False


def _iter_tables(
    root: symtable.SymbolTable,
) -> list[tuple[symtable.SymbolTable, int | None, int]]:
    rows: list[tuple[symtable.SymbolTable, int | None, int]] = []
    stack: list[tuple[symtable.SymbolTable, int | None, int]] = [(root, None, 0)]
    while stack:
        table, parent_id, depth = stack.pop()
        rows.append((table, parent_id, depth))
        children = list(table.get_children())
        table_id = int(table.get_id())
        stack.extend((child, table_id, depth + 1) for child in reversed(children))
    return rows


def _table_for_line(
    tables: Iterable[tuple[symtable.SymbolTable, int | None, int]],
    *,
    anchor_line: int,
) -> symtable.SymbolTable | None:
    chosen: tuple[symtable.SymbolTable, int, int] | None = None
    for table, _parent_id, depth in tables:
        line = _safe_table_lineno(table)
        if line <= 0 or line > anchor_line:
            continue
        if chosen is None:
            chosen = (table, line, depth)
            continue
        _table, chosen_line, chosen_depth = chosen
        if line > chosen_line or (line == chosen_line and depth >= chosen_depth):
            chosen = (table, line, depth)
    if chosen is None:
        return None
    return chosen[0]


def build_sym_scope_graph(table: symtable.SymbolTable) -> dict[str, object]:
    """Build normalized scope graph rows for a root symbol table.

    Returns:
        dict[str, object]: Scope graph table and edge payload.
    """
    rows: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    for current, parent_id, depth in _iter_tables(table):
        table_id = int(current.get_id())
        children = tuple(current.get_children())
        rows.append(
            {
                "table_id": table_id,
                "parent_table_id": parent_id,
                "depth": depth,
                "type": _safe_table_type(current),
                "name": current.get_name(),
                "lineno": _safe_table_lineno(current),
                "is_nested": _safe_is_nested(current),
                "is_optimized": _safe_is_optimized(current),
                "children_count": len(children),
                "symbols_count": len(tuple(current.get_symbols())),
            }
        )
        edges.extend(
            {
                "from_table_id": table_id,
                "to_table_id": int(child.get_id()),
                "edge_kind": "child_scope",
            }
            for child in children
        )
    return {
        "root_table_id": int(table.get_id()),
        "tables": rows,
        "namespace_edges": edges,
        "tables_count": len(rows),
    }


def resolve_binding_id(
    root: symtable.SymbolTable,
    scope: symtable.SymbolTable,
    name: str,
) -> str:
    """Resolve canonical binding identity across symtable scopes.

    Returns:
        str: Canonical binding ID or unresolved sentinel.
    """
    if not name:
        return "UNRESOLVED:"
    try:
        symbol = scope.lookup(name)
    except KeyError:
        return f"UNRESOLVED:{name}"
    except (RuntimeError, TypeError, ValueError):
        return f"UNRESOLVED:{name}"

    root_id = int(root.get_id())
    scope_id = int(scope.get_id())
    if _bool_symbol_attr(symbol, "is_declared_global") or _bool_symbol_attr(symbol, "is_global"):
        return f"{root_id}:{name}"
    binding_attrs = (
        "is_parameter",
        "is_local",
        "is_imported",
        "is_assigned",
        "is_nonlocal",
        "is_free",
    )
    if any(_bool_symbol_attr(symbol, attr_name) for attr_name in binding_attrs):
        return f"{scope_id}:{name}"
    return f"UNRESOLVED:{name}"


def _init_partitions() -> dict[str, list[dict[str, object]]]:
    return {
        "locals": [],
        "globals": [],
        "nonlocals": [],
        "free": [],
        "imported": [],
        "assigned": [],
        "parameters": [],
        "referenced": [],
    }


def _symbol_row(
    *,
    root: symtable.SymbolTable,
    table: symtable.SymbolTable,
    table_id: int,
    symbol: symtable.Symbol,
) -> dict[str, object]:
    name = symbol.get_name()
    row: dict[str, object] = {
        "table_id": table_id,
        "name": name,
        "binding_id": resolve_binding_id(root, table, name),
        "is_local": _bool_symbol_attr(symbol, "is_local"),
        "is_global": _bool_symbol_attr(symbol, "is_global")
        or _bool_symbol_attr(symbol, "is_declared_global"),
        "is_nonlocal": _bool_symbol_attr(symbol, "is_nonlocal"),
        "is_free": _bool_symbol_attr(symbol, "is_free"),
        "is_imported": _bool_symbol_attr(symbol, "is_imported"),
        "is_assigned": _bool_symbol_attr(symbol, "is_assigned"),
        "is_parameter": _bool_symbol_attr(symbol, "is_parameter"),
        "is_referenced": _bool_symbol_attr(symbol, "is_referenced"),
    }
    return row


def _partition_names_for_row(row: dict[str, object]) -> tuple[str, ...]:
    mappings = (
        ("locals", "is_local"),
        ("globals", "is_global"),
        ("nonlocals", "is_nonlocal"),
        ("free", "is_free"),
        ("imported", "is_imported"),
        ("assigned", "is_assigned"),
        ("parameters", "is_parameter"),
        ("referenced", "is_referenced"),
    )
    return tuple(partition for partition, key in mappings if bool(row.get(key)))


def _namespace_edges_for_symbol(
    *,
    table_id: int,
    symbol_name: str,
    symbol: symtable.Symbol,
    namespace_seen: set[tuple[int, int, str]],
) -> list[dict[str, object]]:
    try:
        namespaces = tuple(symbol.get_namespaces())
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return []
    rows: list[dict[str, object]] = []
    for namespace in namespaces:
        target_id = int(namespace.get_id())
        edge_key = (table_id, target_id, symbol_name)
        if edge_key in namespace_seen:
            continue
        namespace_seen.add(edge_key)
        rows.append(
            {
                "from_table_id": table_id,
                "to_table_id": target_id,
                "name": symbol_name,
                "edge_kind": "namespace_ref",
            }
        )
    return rows


def _symbol_rows(
    root: symtable.SymbolTable,
    tables: Iterable[tuple[symtable.SymbolTable, int | None, int]],
) -> tuple[list[dict[str, object]], dict[str, list[dict[str, object]]], list[dict[str, object]]]:
    rows: list[dict[str, object]] = []
    partitions = _init_partitions()
    namespace_edges: list[dict[str, object]] = []
    namespace_seen: set[tuple[int, int, str]] = set()

    for table, _parent_id, _depth in tables:
        table_id = int(table.get_id())
        for symbol in tuple(table.get_symbols()):
            row = _symbol_row(
                root=root,
                table=table,
                table_id=table_id,
                symbol=symbol,
            )
            rows.append(row)
            for partition_name in _partition_names_for_row(row):
                partitions[partition_name].append(row)
            symbol_name = str(row.get("name", ""))
            namespace_edges.extend(
                _namespace_edges_for_symbol(
                    table_id=table_id,
                    symbol_name=symbol_name,
                    symbol=symbol,
                    namespace_seen=namespace_seen,
                )
            )
    return rows, partitions, namespace_edges


def build_incremental_symtable_plane(
    root: symtable.SymbolTable,
    *,
    anchor_name: str,
    anchor_line: int,
) -> dict[str, object]:
    """Build full incremental symtable plane payload for one anchor.

    Returns:
        dict[str, object]: Symtable-plane enrichment payload.
    """
    table_rows = _iter_tables(root)
    scope_graph = build_sym_scope_graph(root)
    symbols, partitions, namespace_edges = _symbol_rows(root, table_rows)
    scope_graph_edges = scope_graph.get("namespace_edges")
    if isinstance(scope_graph_edges, list):
        scope_graph_edges.extend(namespace_edges)
    else:
        scope_graph["namespace_edges"] = list(namespace_edges)

    anchor_scope = _table_for_line(table_rows, anchor_line=anchor_line)
    anchor_binding_id = (
        resolve_binding_id(root, anchor_scope, anchor_name)
        if anchor_scope is not None
        else f"UNRESOLVED:{anchor_name}"
    )
    anchor_candidates = [row for row in symbols if row.get("name") == anchor_name]
    return {
        "scope_graph": scope_graph,
        "partitions": {key: value for key, value in partitions.items() if isinstance(key, str)},
        "symbol_rows": symbols,
        "binding_resolution": {
            "symbol": anchor_name,
            "anchor_line": anchor_line,
            "anchor_table_id": int(anchor_scope.get_id()) if anchor_scope is not None else None,
            "binding_id": anchor_binding_id,
            "status": "resolved" if not anchor_binding_id.startswith("UNRESOLVED:") else "missing",
            "candidates": anchor_candidates,
            "candidates_count": len(anchor_candidates),
        },
    }


__all__ = [
    "build_incremental_symtable_plane",
    "build_sym_scope_graph",
    "resolve_binding_id",
]
