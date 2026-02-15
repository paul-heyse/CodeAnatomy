"""Structured Rust module graph builder from normalized tree-sitter fact rows."""

from __future__ import annotations

from collections import OrderedDict

import msgspec

from tools.cq.search.rust.module_graph_contracts import (
    RustImportEdgeV1,
    RustModuleGraphV1,
    RustModuleNodeV1,
)


def _string(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def _module_name(row: dict[str, object]) -> str | None:
    return _string(row.get("module_name")) or _string(row.get("name"))


def _normalize_modules(module_rows: list[dict[str, object]]) -> tuple[RustModuleNodeV1, ...]:
    rows: OrderedDict[str, RustModuleNodeV1] = OrderedDict()
    for row in module_rows:
        if not isinstance(row, dict):
            continue
        module_name = _module_name(row)
        if module_name is None:
            continue
        module_id = _string(row.get("module_id")) or f"module:{module_name}"
        file_path = _string(row.get("file_path"))
        rows[module_id] = RustModuleNodeV1(
            module_id=module_id,
            module_name=module_name,
            file_path=file_path,
        )
    return tuple(rows.values())


def _module_lookup(modules: tuple[RustModuleNodeV1, ...]) -> dict[str, str]:
    return {row.module_name: row.module_id for row in modules}


def _normalize_import_edges(
    import_rows: list[dict[str, object]],
    modules: tuple[RustModuleNodeV1, ...],
) -> tuple[RustImportEdgeV1, ...]:
    lookup = _module_lookup(modules)
    default_source = modules[0].module_id if modules else "module:<root>"
    edges: OrderedDict[tuple[str, str, str, bool], RustImportEdgeV1] = OrderedDict()
    for row in import_rows:
        if not isinstance(row, dict):
            continue
        target_path = _string(row.get("target_path")) or _string(row.get("path"))
        if target_path is None:
            continue
        source_module_id = _string(row.get("source_module_id"))
        if source_module_id is None:
            source_module_id = default_source
            for module_name, module_id in lookup.items():
                if target_path.startswith(f"{module_name}::"):
                    source_module_id = module_id
                    break
        visibility = _string(row.get("visibility")) or "private"
        is_reexport = bool(row.get("is_reexport"))
        edge = RustImportEdgeV1(
            source_module_id=source_module_id,
            target_path=target_path,
            visibility=visibility,
            is_reexport=is_reexport,
        )
        edges[edge.source_module_id, edge.target_path, edge.visibility, edge.is_reexport] = edge
    return tuple(edges.values())


def build_module_graph(
    *,
    module_rows: list[dict[str, object]],
    import_rows: list[dict[str, object]],
) -> dict[str, object]:
    """Build normalized module graph payload from loose fact rows."""
    modules = _normalize_modules(module_rows)
    edges = _normalize_import_edges(import_rows, modules)
    graph = RustModuleGraphV1(
        modules=modules,
        edges=edges,
        metadata={
            "module_count": len(modules),
            "edge_count": len(edges),
        },
    )
    return msgspec.to_builtins(graph)


__all__ = ["build_module_graph"]
