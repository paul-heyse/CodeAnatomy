"""Shared utilities for normalization pipelines."""

from __future__ import annotations

from collections.abc import Mapping

from arrowdsl.compute.ids import (
    HashSpec,
    SpanIdSpec,
    add_span_id_column,
    hash_column_values,
    masked_prefixed_hash,
    prefixed_hash64,
    prefixed_hash_id,
    span_id,
    stable_id,
    stable_int64,
)
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike
from arrowdsl.plan.joins import code_unit_meta_config, left_join
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan_bundle
from arrowdsl.plan_helpers import (
    PlanSource,
    encoding_columns_from_metadata,
    encoding_projection,
    finalize_plan,
    finalize_plan_result,
    flatten_struct_field,
    plan_source,
    project_columns,
    query_for_schema,
)
from arrowdsl.schema.metadata import (
    DICT_INDEX_META,
    DICT_ORDERED_META,
    ENCODING_DICTIONARY,
    ENCODING_META,
    encoding_policy_from_schema,
)
from arrowdsl.schema.ops import align_plan as align_plan_to_schema_helper
from normalize.registry_ids import (
    DEF_USE_EVENT_ID_SPEC,
    DIAG_ID_SPEC,
    REACH_EDGE_ID_SPEC,
    TYPE_EXPR_ID_SPEC,
    TYPE_ID_SPEC,
)
from schema_spec.specs import dict_field


def join_code_unit_meta(table: TableLike, code_units: TableLike) -> TableLike:
    """Left-join code unit metadata (file_id/path) onto a table.

    Returns
    -------
    TableLike
        Table with code unit metadata columns.
    """
    config = code_unit_meta_config(table.column_names, code_units.column_names)
    if config is None:
        return table
    return left_join(table, code_units, config=config, use_threads=True)


def align_plan_to_schema(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
    keep_extra_columns: bool = False,
) -> Plan:
    """Align a plan to a target schema via projection.

    Returns
    -------
    Plan
        Plan projecting/casting to match the schema.
    """
    return align_plan_to_schema_helper(
        plan,
        schema=schema,
        ctx=ctx,
        keep_extra_columns=keep_extra_columns,
    )


def finalize_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Finalize a bundle of plans into tables or readers.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Finalized plan outputs keyed by name.
    """
    return run_plan_bundle(plans, ctx=ctx, prefer_reader=prefer_reader)


__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_ID_SPEC",
    "DICT_INDEX_META",
    "DICT_ORDERED_META",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
    "HashSpec",
    "PlanSource",
    "SpanIdSpec",
    "add_span_id_column",
    "align_plan_to_schema",
    "dict_field",
    "encoding_columns_from_metadata",
    "encoding_policy_from_schema",
    "encoding_projection",
    "finalize_plan",
    "finalize_plan_bundle",
    "finalize_plan_result",
    "flatten_struct_field",
    "hash_column_values",
    "join_code_unit_meta",
    "masked_prefixed_hash",
    "plan_source",
    "prefixed_hash64",
    "prefixed_hash_id",
    "project_columns",
    "query_for_schema",
    "span_id",
    "stable_id",
    "stable_int64",
]
