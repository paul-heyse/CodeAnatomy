"""QuerySpec registry for CPG datasets."""

from __future__ import annotations

from arrowdsl.plan.query import QuerySpec
from cpg.schemas import CPG_EDGES_SPEC, CPG_NODES_SPEC, CPG_PROPS_SPEC

CPG_NODES_QUERY: QuerySpec = CPG_NODES_SPEC.query()
CPG_EDGES_QUERY: QuerySpec = CPG_EDGES_SPEC.query()
CPG_PROPS_QUERY: QuerySpec = CPG_PROPS_SPEC.query()


def cpg_query_specs() -> dict[str, QuerySpec]:
    """Return a mapping of CPG dataset names to query specs.

    Returns
    -------
    dict[str, QuerySpec]
        Mapping of dataset names to QuerySpec instances.
    """
    return {
        "cpg_nodes": CPG_NODES_QUERY,
        "cpg_edges": CPG_EDGES_QUERY,
        "cpg_props": CPG_PROPS_QUERY,
    }


__all__ = [
    "CPG_EDGES_QUERY",
    "CPG_NODES_QUERY",
    "CPG_PROPS_QUERY",
    "cpg_query_specs",
]
