"""CPG builders, schemas, and kind enums."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cpg.build_edges import EdgeBuildOptions, build_cpg_edges, build_cpg_edges_raw
    from cpg.build_nodes import NodeBuildOptions, build_cpg_nodes, build_cpg_nodes_raw
    from cpg.build_props import PropsBuildOptions, build_cpg_props, build_cpg_props_raw
    from cpg.constants import CpgBuildArtifacts
    from cpg.kinds_ultimate import (
        EdgeKind,
        EntityKind,
        NodeKind,
        validate_derivation_extractors,
        validate_registry_completeness,
    )
    from cpg.registry import CpgRegistry, default_cpg_registry
    from cpg.schemas import (
        CPG_EDGES_CONTRACT,
        CPG_EDGES_SCHEMA,
        CPG_NODES_CONTRACT,
        CPG_NODES_SCHEMA,
        CPG_PROPS_CONTRACT,
        CPG_PROPS_SCHEMA,
        SCHEMA_VERSION,
        empty_edges,
        empty_nodes,
        empty_props,
    )

_IMPORT_MAP: dict[str, tuple[str, str]] = {
    "EdgeBuildOptions": ("cpg.build_edges", "EdgeBuildOptions"),
    "build_cpg_edges": ("cpg.build_edges", "build_cpg_edges"),
    "build_cpg_edges_raw": ("cpg.build_edges", "build_cpg_edges_raw"),
    "NodeBuildOptions": ("cpg.build_nodes", "NodeBuildOptions"),
    "build_cpg_nodes": ("cpg.build_nodes", "build_cpg_nodes"),
    "build_cpg_nodes_raw": ("cpg.build_nodes", "build_cpg_nodes_raw"),
    "PropsBuildOptions": ("cpg.build_props", "PropsBuildOptions"),
    "build_cpg_props": ("cpg.build_props", "build_cpg_props"),
    "build_cpg_props_raw": ("cpg.build_props", "build_cpg_props_raw"),
    "CpgBuildArtifacts": ("cpg.constants", "CpgBuildArtifacts"),
    "EdgeKind": ("cpg.kinds_ultimate", "EdgeKind"),
    "EntityKind": ("cpg.kinds_ultimate", "EntityKind"),
    "NodeKind": ("cpg.kinds_ultimate", "NodeKind"),
    "validate_derivation_extractors": ("cpg.kinds_ultimate", "validate_derivation_extractors"),
    "validate_registry_completeness": ("cpg.kinds_ultimate", "validate_registry_completeness"),
    "CpgRegistry": ("cpg.registry", "CpgRegistry"),
    "default_cpg_registry": ("cpg.registry", "default_cpg_registry"),
    "CPG_EDGES_CONTRACT": ("cpg.schemas", "CPG_EDGES_CONTRACT"),
    "CPG_EDGES_SCHEMA": ("cpg.schemas", "CPG_EDGES_SCHEMA"),
    "CPG_NODES_CONTRACT": ("cpg.schemas", "CPG_NODES_CONTRACT"),
    "CPG_NODES_SCHEMA": ("cpg.schemas", "CPG_NODES_SCHEMA"),
    "CPG_PROPS_CONTRACT": ("cpg.schemas", "CPG_PROPS_CONTRACT"),
    "CPG_PROPS_SCHEMA": ("cpg.schemas", "CPG_PROPS_SCHEMA"),
    "SCHEMA_VERSION": ("cpg.schemas", "SCHEMA_VERSION"),
    "empty_edges": ("cpg.schemas", "empty_edges"),
    "empty_nodes": ("cpg.schemas", "empty_nodes"),
    "empty_props": ("cpg.schemas", "empty_props"),
}


def __getattr__(name: str) -> object:
    if name in _IMPORT_MAP:
        module_name, attr_name = _IMPORT_MAP[name]
        module = importlib.import_module(module_name)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


__all__ = [
    "CPG_EDGES_CONTRACT",
    "CPG_EDGES_SCHEMA",
    "CPG_NODES_CONTRACT",
    "CPG_NODES_SCHEMA",
    "CPG_PROPS_CONTRACT",
    "CPG_PROPS_SCHEMA",
    "SCHEMA_VERSION",
    "CpgBuildArtifacts",
    "CpgRegistry",
    "EdgeBuildOptions",
    "EdgeKind",
    "EntityKind",
    "NodeBuildOptions",
    "NodeKind",
    "PropsBuildOptions",
    "build_cpg_edges",
    "build_cpg_edges_raw",
    "build_cpg_nodes",
    "build_cpg_nodes_raw",
    "build_cpg_props",
    "build_cpg_props_raw",
    "default_cpg_registry",
    "empty_edges",
    "empty_nodes",
    "empty_props",
    "validate_derivation_extractors",
    "validate_registry_completeness",
]
