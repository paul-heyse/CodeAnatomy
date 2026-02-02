"""Reusable Hamilton subDAGs for pipeline nodes.

**EXECUTION AUTHORITY PATTERN**

This module contains Hamilton subDAGs that **consume** semantic views registered
by the view graph infrastructure. Hamilton does NOT re-register these views.

The single source of truth for view registration is:
- ``datafusion_engine.views.registry_specs._semantics_view_nodes()``

Hamilton nodes here use ``source()`` to reference views that are already
registered in the DataFusion session context via ``ensure_view_graph()``.
This separation ensures:

1. No duplicate view registration
2. Clear ownership: View Graph owns registration, Hamilton owns execution
3. Consistent schema contracts across both systems

See Also
--------
datafusion_engine.views.registry_specs : View registration entry point.
hamilton_pipeline.driver_factory.build_view_graph_context : View graph setup.
"""

from __future__ import annotations

from hamilton.function_modifiers import parameterized_subdag, schema, source, tag_outputs, value

from cpg.emit_specs import _EDGE_OUTPUT_COLUMNS, _NODE_OUTPUT_COLUMNS, _PROP_OUTPUT_COLUMNS
from hamilton_pipeline.modules import cpg_finalize as cpg_finalize_module
from hamilton_pipeline.modules import cpg_outputs as cpg_outputs_module
from hamilton_pipeline.tag_policy import apply_tag, semantic_tag_policy, tag_outputs_by_name
from relspec.runtime_artifacts import TableLike


@parameterized_subdag(
    cpg_finalize_module,
    inputs={"runtime_profile_spec": source("runtime_profile_spec")},
    cpg_nodes_final={
        "inputs": {
            "table": source("cpg_nodes_v1"),
            "table_name": value("cpg_nodes_v1"),
        }
    },
    cpg_edges_final={
        "inputs": {
            "table": source("cpg_edges_v1"),
            "table_name": value("cpg_edges_v1"),
        }
    },
    cpg_props_final={
        "inputs": {
            "table": source("cpg_props_v1"),
            "table_name": value("cpg_props_v1"),
        }
    },
)
@tag_outputs(
    **tag_outputs_by_name(
        ("cpg_nodes_final", "cpg_edges_final", "cpg_props_final"),
        layer="execution",
        kind="table",
    )
)
def cpg_final_tables(final_table: TableLike) -> TableLike:
    """Finalize CPG outputs via parameterized sub-DAGs.

    Returns
    -------
    TableLike
        Finalized output table for the parameterized sub-DAG.
    """
    return final_table


@parameterized_subdag(
    cpg_outputs_module,
    cpg_nodes_raw={
        "inputs": {
            "table": source("cpg_nodes_final"),
            "dataset_name": value("cpg_nodes_v1"),
        }
    },
    cpg_edges_raw={
        "inputs": {
            "table": source("cpg_edges_final"),
            "dataset_name": value("cpg_edges_v1"),
        }
    },
    cpg_props_raw={
        "inputs": {
            "table": source("cpg_props_final"),
            "dataset_name": value("cpg_props_v1"),
        }
    },
)
def cpg_output_tables(table: TableLike) -> TableLike:
    """Return parameterized CPG outputs with shared validation.

    Returns
    -------
    TableLike
        Shared output table for the parameterized subdag.
    """
    return table


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


@apply_tag(semantic_tag_policy("cpg_nodes"))
@schema.output(*_CPG_NODES_SCHEMA)
def cpg_nodes(cpg_nodes_raw: TableLike) -> TableLike:
    """Return the canonical CPG nodes output table.

    Returns
    -------
    TableLike
        CPG nodes output table.
    """
    return cpg_nodes_raw


@apply_tag(semantic_tag_policy("cpg_edges"))
@schema.output(*_CPG_EDGES_SCHEMA)
def cpg_edges(cpg_edges_raw: TableLike) -> TableLike:
    """Return the canonical CPG edges output table.

    Returns
    -------
    TableLike
        CPG edges output table.
    """
    return cpg_edges_raw


@apply_tag(semantic_tag_policy("cpg_props"))
@schema.output(*_CPG_PROPS_SCHEMA)
def cpg_props(cpg_props_raw: TableLike) -> TableLike:
    """Return the canonical CPG properties output table.

    Returns
    -------
    TableLike
        CPG properties output table.
    """
    return cpg_props_raw


@apply_tag(semantic_tag_policy("cpg_props_map"))
@schema.output(*_CPG_PROPS_MAP_SCHEMA)
def cpg_props_map(cpg_props_map_v1: TableLike) -> TableLike:
    """Return the canonical CPG property map table.

    Returns
    -------
    TableLike
        CPG property map table.
    """
    return cpg_props_map_v1


@apply_tag(semantic_tag_policy("cpg_edges_by_src"))
@schema.output(*_CPG_EDGES_BY_SRC_SCHEMA)
def cpg_edges_by_src(cpg_edges_by_src_v1: TableLike) -> TableLike:
    """Return CPG edges grouped by source node.

    Returns
    -------
    TableLike
        CPG edges-by-source table.
    """
    return cpg_edges_by_src_v1


@apply_tag(semantic_tag_policy("cpg_edges_by_dst"))
@schema.output(*_CPG_EDGES_BY_DST_SCHEMA)
def cpg_edges_by_dst(cpg_edges_by_dst_v1: TableLike) -> TableLike:
    """Return CPG edges grouped by destination node.

    Returns
    -------
    TableLike
        CPG edges-by-destination table.
    """
    return cpg_edges_by_dst_v1


__all__ = [
    "cpg_edges",
    "cpg_edges_by_dst",
    "cpg_edges_by_src",
    "cpg_final_tables",
    "cpg_nodes",
    "cpg_output_tables",
    "cpg_props",
    "cpg_props_map",
]
