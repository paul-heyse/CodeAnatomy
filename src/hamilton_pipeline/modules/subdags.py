"""Reusable Hamilton subDAGs for pipeline nodes.

**EXECUTION AUTHORITY PATTERN**

This module contains Hamilton subDAGs that **consume** semantic views registered
by the view graph infrastructure. Hamilton does NOT re-register these views.

The single source of truth for view registration is:
- ``datafusion_engine.views.registry_specs._semantics_view_nodes()``

Hamilton nodes here directly consume views that are already registered in the
DataFusion session context via ``ensure_view_graph()``. This separation ensures:

1. No duplicate view registration
2. Clear ownership: View Graph owns registration, Hamilton owns execution
3. Consistent schema contracts across both systems

See Also:
--------
datafusion_engine.views.registry_specs : View registration entry point.
hamilton_pipeline.driver_factory.build_view_graph_context : View graph setup.
"""

from __future__ import annotations

from hamilton.function_modifiers import schema

from cpg.emit_specs import _EDGE_OUTPUT_COLUMNS, _NODE_OUTPUT_COLUMNS, _PROP_OUTPUT_COLUMNS
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from relspec.runtime_artifacts import TableLike

_CPG_NODES_SCHEMA = tuple((col, "string") for col in _NODE_OUTPUT_COLUMNS)
_CPG_EDGES_SCHEMA = tuple((col, "string") for col in _EDGE_OUTPUT_COLUMNS)
_CPG_PROPS_SCHEMA = tuple((col, "string") for col in _PROP_OUTPUT_COLUMNS)
_CPG_PROPS_MAP_SCHEMA = (
    ("entity_kind", "string"),
    ("entity_id", "string"),
    ("node_kind", "string"),
    ("props", "string"),
)
_CPG_EDGES_BY_SRC_SCHEMA = (("src_node_id", "string"), ("edges", "string"))
_CPG_EDGES_BY_DST_SCHEMA = (("dst_node_id", "string"), ("edges", "string"))


@apply_tag(TagPolicy(layer="semantic", kind="table", artifact="cpg_nodes"))
@schema.output(*_CPG_NODES_SCHEMA)
def cpg_nodes(cpg_nodes_v1: TableLike) -> TableLike:
    """Return the canonical CPG nodes output table.

    Returns:
    -------
    TableLike
        CPG nodes output table.
    """
    return cpg_nodes_v1


@apply_tag(TagPolicy(layer="semantic", kind="table", artifact="cpg_edges"))
@schema.output(*_CPG_EDGES_SCHEMA)
def cpg_edges(cpg_edges_v1: TableLike) -> TableLike:
    """Return the canonical CPG edges output table.

    Returns:
    -------
    TableLike
        CPG edges output table.
    """
    return cpg_edges_v1


@apply_tag(TagPolicy(layer="semantic", kind="table", artifact="cpg_props"))
@schema.output(*_CPG_PROPS_SCHEMA)
def cpg_props(cpg_props_v1: TableLike) -> TableLike:
    """Return the canonical CPG properties output table.

    Returns:
    -------
    TableLike
        CPG properties output table.
    """
    return cpg_props_v1


@apply_tag(TagPolicy(layer="semantic", kind="table", artifact="cpg_props_map"))
@schema.output(*_CPG_PROPS_MAP_SCHEMA)
def cpg_props_map(cpg_props_map_v1: TableLike) -> TableLike:
    """Return the canonical CPG property map table.

    Returns:
    -------
    TableLike
        CPG property map table.
    """
    return cpg_props_map_v1


@apply_tag(TagPolicy(layer="semantic", kind="table", artifact="cpg_edges_by_src"))
@schema.output(*_CPG_EDGES_BY_SRC_SCHEMA)
def cpg_edges_by_src(cpg_edges_by_src_v1: TableLike) -> TableLike:
    """Return CPG edges grouped by source node.

    Returns:
    -------
    TableLike
        CPG edges-by-source table.
    """
    return cpg_edges_by_src_v1


@apply_tag(TagPolicy(layer="semantic", kind="table", artifact="cpg_edges_by_dst"))
@schema.output(*_CPG_EDGES_BY_DST_SCHEMA)
def cpg_edges_by_dst(cpg_edges_by_dst_v1: TableLike) -> TableLike:
    """Return CPG edges grouped by destination node.

    Returns:
    -------
    TableLike
        CPG edges-by-destination table.
    """
    return cpg_edges_by_dst_v1


__all__ = [
    "cpg_edges",
    "cpg_edges_by_dst",
    "cpg_edges_by_src",
    "cpg_nodes",
    "cpg_props",
    "cpg_props_map",
]
