from __future__ import annotations

from .build_edges import (
    EdgeBuildOptions,
    build_cpg_edges,
    build_cpg_edges_raw,
)
from .build_nodes import (
    NodeBuildOptions,
    build_cpg_nodes,
    build_cpg_nodes_raw,
)
from .build_props import (
    PropsBuildOptions,
    build_cpg_props,
    build_cpg_props_raw,
)
from .kinds import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
    EntityKind,
    NodeKind,
)
from .schemas import (
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

__all__ = [
    # kinds
    "NodeKind",
    "EdgeKind",
    "EntityKind",
    "SCIP_ROLE_DEFINITION",
    "SCIP_ROLE_IMPORT",
    "SCIP_ROLE_WRITE",
    "SCIP_ROLE_READ",
    # schemas + contracts
    "SCHEMA_VERSION",
    "CPG_NODES_SCHEMA",
    "CPG_EDGES_SCHEMA",
    "CPG_PROPS_SCHEMA",
    "CPG_NODES_CONTRACT",
    "CPG_EDGES_CONTRACT",
    "CPG_PROPS_CONTRACT",
    "empty_nodes",
    "empty_edges",
    "empty_props",
    # builders
    "NodeBuildOptions",
    "EdgeBuildOptions",
    "PropsBuildOptions",
    "build_cpg_nodes_raw",
    "build_cpg_nodes",
    "build_cpg_edges_raw",
    "build_cpg_edges",
    "build_cpg_props_raw",
    "build_cpg_props",
]
