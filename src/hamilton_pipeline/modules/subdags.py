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

from hamilton.function_modifiers import parameterized_subdag, source, tag_outputs, value

from hamilton_pipeline.modules import cpg_finalize as cpg_finalize_module
from hamilton_pipeline.modules import cpg_outputs as cpg_outputs_module
from hamilton_pipeline.tag_policy import TagPolicy, tag_outputs_by_name, tag_outputs_payloads
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


_CPG_OUTPUT_POLICIES = {
    "cpg_nodes": TagPolicy(
        layer="semantic",
        kind="table",
        artifact="cpg_nodes",
        semantic_id="cpg.nodes.v1",
        entity="node",
        grain="per_node",
        version="1",
        stability="design",
        schema_ref="semantic.cpg_nodes_v1",
        materialization="delta",
        materialized_name="semantic.cpg_nodes_v1",
        entity_keys=("repo", "commit", "node_id"),
        join_keys=("repo", "commit", "node_id"),
    ),
    "cpg_edges": TagPolicy(
        layer="semantic",
        kind="table",
        artifact="cpg_edges",
        semantic_id="cpg.edges.v1",
        entity="edge",
        grain="per_edge",
        version="1",
        stability="design",
        schema_ref="semantic.cpg_edges_v1",
        materialization="delta",
        materialized_name="semantic.cpg_edges_v1",
        entity_keys=("repo", "commit", "edge_id"),
        join_keys=("repo", "commit", "edge_id"),
    ),
    "cpg_props": TagPolicy(
        layer="semantic",
        kind="table",
        artifact="cpg_props",
        semantic_id="cpg.props.v1",
        entity="prop",
        grain="per_prop",
        version="1",
        stability="design",
        schema_ref="semantic.cpg_props_v1",
        materialization="delta",
        materialized_name="semantic.cpg_props_v1",
        entity_keys=("repo", "commit", "node_id", "key"),
        join_keys=("repo", "commit", "node_id"),
    ),
}


@parameterized_subdag(
    cpg_outputs_module,
    cpg_nodes={
        "inputs": {
            "table": source("cpg_nodes_final"),
            "dataset_name": value("cpg_nodes_v1"),
        }
    },
    cpg_edges={
        "inputs": {
            "table": source("cpg_edges_final"),
            "dataset_name": value("cpg_edges_v1"),
        }
    },
    cpg_props={
        "inputs": {
            "table": source("cpg_props_final"),
            "dataset_name": value("cpg_props_v1"),
        }
    },
)
@tag_outputs(**tag_outputs_payloads(_CPG_OUTPUT_POLICIES))
def cpg_output_tables(table: TableLike) -> TableLike:
    """Return parameterized CPG outputs with shared validation.

    Returns
    -------
    TableLike
        Shared output table for the parameterized subdag.
    """
    return table


__all__ = ["cpg_final_tables", "cpg_output_tables"]
