"""Plan helper utilities and QuerySpec registry for CPG datasets."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.plan.scan_io import plan_from_dataset
from arrowdsl.plan_helpers import (
    align_plan,
    align_table_to_schema,
    assert_schema_metadata,
    empty_plan,
    encoding_columns_from_metadata,
    ensure_plan,
    finalize_context_for_plan,
    finalize_plan,
    set_or_append_column,
)
from arrowdsl.plan_helpers import encode_plan as plan_encode_plan
from arrowdsl.schema.schema import EncodingSpec, unify_schema_with_metadata
from cpg.schemas import CPG_EDGES_SPEC, CPG_NODES_SPEC, CPG_PROPS_SPEC

CPG_NODES_QUERY: QuerySpec = CPG_NODES_SPEC.query()
CPG_EDGES_QUERY: QuerySpec = CPG_EDGES_SPEC.query()
CPG_PROPS_QUERY: QuerySpec = CPG_PROPS_SPEC.query()


def encode_plan(
    plan: Plan,
    *,
    specs: Sequence[EncodingSpec],
    ctx: ExecutionContext,
) -> Plan:
    """Return a plan with dictionary encoding applied.

    Returns
    -------
    Plan
        Plan with dictionary-encoded columns.
    """
    encode_cols = tuple(spec.column for spec in specs)
    return plan_encode_plan(plan, columns=encode_cols, ctx=ctx)


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
    "align_plan",
    "align_table_to_schema",
    "assert_schema_metadata",
    "cpg_query_specs",
    "empty_plan",
    "encode_plan",
    "encoding_columns_from_metadata",
    "ensure_plan",
    "finalize_context_for_plan",
    "finalize_plan",
    "plan_from_dataset",
    "set_or_append_column",
    "unify_schema_with_metadata",
]
