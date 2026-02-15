"""Rust module/import graph construction from tree-sitter evidence payloads."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.search.rust_module_graph_contracts import RustModuleEdgeV1, RustModuleGraphV1


def build_rust_module_graph(payload: Mapping[str, object]) -> RustModuleGraphV1:
    """Build module graph rows from rust tree-sitter fact payloads.

    Returns:
        RustModuleGraphV1: Module/import graph projection.
    """
    facts = payload.get("rust_tree_sitter_facts")
    if not isinstance(facts, dict):
        return RustModuleGraphV1()

    modules_raw = facts.get("modules")
    imports_raw = facts.get("imports")
    modules = tuple(value for value in (modules_raw or ()) if isinstance(value, str))
    imports = tuple(value for value in (imports_raw or ()) if isinstance(value, str))

    edges: list[RustModuleEdgeV1] = []
    for module_name in modules:
        prefix = f"{module_name}::"
        edges.extend(
            RustModuleEdgeV1(source=module_name, target=import_name)
            for import_name in imports
            if import_name.startswith(prefix)
        )
    return RustModuleGraphV1(modules=modules, imports=imports, edges=tuple(edges))


def attach_rust_module_graph(payload: dict[str, object]) -> dict[str, object]:
    """Attach serialized module graph rows to enrichment payload.

    Returns:
        dict[str, object]: Updated payload.
    """
    graph = build_rust_module_graph(payload)
    payload["rust_module_graph"] = msgspec.to_builtins(graph)
    return payload


__all__ = ["attach_rust_module_graph", "build_rust_module_graph"]
