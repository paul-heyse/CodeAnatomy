"""Symtable binding resolution helpers for CPG edges."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

import pyarrow as pa

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, coerce_table_like
from arrowdsl.schema.build import iter_rows_from_table

_RESOLUTION_CONFIDENCE: Final[dict[str, float]] = {
    "GLOBAL": 0.9,
    "NONLOCAL": 0.85,
    "FREE": 0.75,
}


def build_binding_resolution_table(
    scopes: TableLike | RecordBatchReaderLike,
    scope_edges: TableLike | RecordBatchReaderLike,
    bindings: TableLike | RecordBatchReaderLike,
) -> pa.Table:
    """Build binding resolution edges from symtable metadata.

    Parameters
    ----------
    scopes
        Symtable scope rows (symtable_scopes view).
    scope_edges
        Parent/child scope edges (symtable_scope_edges view).
    bindings
        Symtable binding rows (symtable_bindings view).

    Returns
    -------
    pyarrow.Table
        Binding resolution edge rows with ambiguity grouping.
    """
    scope_table = _to_table(scopes)
    edge_table = _to_table(scope_edges)
    binding_table = _to_table(bindings)
    scope_parent = _scope_parent_map(edge_table)
    scope_type = _scope_type_map(scope_table)
    scope_path = _scope_path_map(scope_table)
    module_scope_by_path = _module_scope_by_path(scope_type, scope_path)
    declared_bindings = _declared_bindings(binding_table)
    rows: list[dict[str, object]] = []
    for row in iter_rows_from_table(binding_table):
        binding_kind = _safe_str(row.get("binding_kind"))
        if binding_kind not in {"global_ref", "nonlocal_ref", "free_ref"}:
            continue
        binding_id = _safe_str(row.get("binding_id"))
        scope_id = _safe_str(row.get("scope_id"))
        name = _safe_str(row.get("name"))
        path = _safe_str(row.get("path"))
        if not binding_id or not scope_id or not name:
            continue
        resolution_kind = _resolution_kind(binding_kind)
        candidates = _resolve_candidates(
            resolution_kind,
            scope_id=scope_id,
            name=name,
            scope_parent=scope_parent,
            scope_type=scope_type,
            declared_bindings=declared_bindings,
            module_scope_by_path=module_scope_by_path,
            path=path,
        )
        if not candidates:
            continue
        ambiguity_group_id = f"{binding_id}:{resolution_kind}"
        confidence = _RESOLUTION_CONFIDENCE.get(resolution_kind, 0.7)
        for outer_scope_id in candidates:
            outer_binding_id = declared_bindings.get(outer_scope_id, {}).get(name)
            if outer_binding_id is None:
                continue
            rows.append(
                {
                    "binding_id": binding_id,
                    "outer_binding_id": outer_binding_id,
                    "kind": resolution_kind,
                    "reason": _resolution_reason(resolution_kind, outer_scope_id, scope_type),
                    "path": path,
                    "ambiguity_group_id": ambiguity_group_id,
                    "confidence": confidence,
                }
            )
    return _rows_to_table(rows)


def _to_table(table: TableLike | RecordBatchReaderLike) -> pa.Table:
    coerced = coerce_table_like(table)
    if isinstance(coerced, RecordBatchReaderLike):
        return coerced.read_all()
    return _ensure_pyarrow_table(coerced)


def _ensure_pyarrow_table(table: TableLike) -> pa.Table:
    if isinstance(table, pa.Table):
        return table
    if isinstance(table, pa.RecordBatch):
        return pa.Table.from_batches([table])
    return pa.table(table)


def _scope_parent_map(edges: pa.Table) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in iter_rows_from_table(edges):
        parent = _safe_str(row.get("parent_scope_id"))
        child = _safe_str(row.get("child_scope_id"))
        if parent and child and child not in mapping:
            mapping[child] = parent
    return mapping


def _scope_type_map(scopes: pa.Table) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in iter_rows_from_table(scopes):
        scope_id = _safe_str(row.get("scope_id"))
        scope_type = _safe_str(row.get("scope_type"))
        if scope_id and scope_type and scope_id not in mapping:
            mapping[scope_id] = scope_type
    return mapping


def _scope_path_map(scopes: pa.Table) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in iter_rows_from_table(scopes):
        scope_id = _safe_str(row.get("scope_id"))
        path = _safe_str(row.get("path"))
        if scope_id and path and scope_id not in mapping:
            mapping[scope_id] = path
    return mapping


def _module_scope_by_path(
    scope_type: dict[str, str],
    scope_path: dict[str, str],
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for scope_id, scope_kind in scope_type.items():
        if scope_kind != "MODULE":
            continue
        path = scope_path.get(scope_id)
        if path and path not in mapping:
            mapping[path] = scope_id
    return mapping


def _declared_bindings(bindings: pa.Table) -> dict[str, dict[str, str]]:
    declared: dict[str, dict[str, str]] = {}
    for row in iter_rows_from_table(bindings):
        declared_here = bool(row.get("declared_here"))
        if not declared_here:
            continue
        scope_id = _safe_str(row.get("scope_id"))
        name = _safe_str(row.get("name"))
        binding_id = _safe_str(row.get("binding_id"))
        if not scope_id or not name or not binding_id:
            continue
        declared.setdefault(scope_id, {})[name] = binding_id
    return declared


def _resolution_kind(binding_kind: str) -> str:
    if binding_kind == "global_ref":
        return "GLOBAL"
    if binding_kind == "nonlocal_ref":
        return "NONLOCAL"
    if binding_kind == "free_ref":
        return "FREE"
    return "UNKNOWN"


def _resolve_candidates(
    resolution_kind: str,
    *,
    scope_id: str,
    name: str,
    scope_parent: dict[str, str],
    scope_type: dict[str, str],
    declared_bindings: dict[str, dict[str, str]],
    module_scope_by_path: dict[str, str],
    path: str,
) -> list[str]:
    if resolution_kind == "GLOBAL":
        module_scope = module_scope_by_path.get(path)
        if module_scope is None:
            return []
        if name in declared_bindings.get(module_scope, {}):
            return [module_scope]
        return []
    if resolution_kind in {"NONLOCAL", "FREE"}:
        candidates = _walk_parent_chain(scope_id, scope_parent)
        matches = [
            candidate
            for candidate in candidates
            if name in declared_bindings.get(candidate, {})
        ]
        if resolution_kind == "NONLOCAL":
            return [
                candidate
                for candidate in matches
                if scope_type.get(candidate) != "MODULE"
            ]
        module_scope = module_scope_by_path.get(path)
        if module_scope and name in declared_bindings.get(module_scope, {}):
            if module_scope not in matches:
                matches.append(module_scope)
        return matches
    return []


def _walk_parent_chain(scope_id: str, scope_parent: dict[str, str]) -> Iterable[str]:
    current = scope_parent.get(scope_id)
    while current is not None:
        yield current
        current = scope_parent.get(current)


def _resolution_reason(
    resolution_kind: str,
    outer_scope_id: str,
    scope_type: dict[str, str],
) -> str:
    if resolution_kind == "GLOBAL":
        return "module_scope"
    if resolution_kind == "NONLOCAL":
        return "enclosing_scope"
    if resolution_kind == "FREE":
        return "enclosing_or_module_scope"
    return scope_type.get(outer_scope_id, "unknown")


def _rows_to_table(rows: list[dict[str, object]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("binding_id", pa.string()),
            pa.field("outer_binding_id", pa.string()),
            pa.field("kind", pa.string()),
            pa.field("reason", pa.string()),
            pa.field("path", pa.string()),
            pa.field("ambiguity_group_id", pa.string()),
            pa.field("confidence", pa.float32()),
        ]
    )
    if not rows:
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema],
            schema=schema,
        )
    return pa.Table.from_pylist(rows, schema=schema)


def _safe_str(value: object) -> str:
    if value is None:
        return ""
    return str(value)


__all__ = ["build_binding_resolution_table"]
