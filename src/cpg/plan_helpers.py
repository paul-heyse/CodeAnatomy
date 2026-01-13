"""Plan helper compatibility layer for CPG plan-lane builders."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.source import plan_from_dataset
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
from arrowdsl.schema.schema import EncodingSpec
from arrowdsl.schema.unify import unify_schema_with_metadata


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


__all__ = [
    "align_plan",
    "align_table_to_schema",
    "assert_schema_metadata",
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
