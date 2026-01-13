"""Plan builders for normalized bytecode DFG tables."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ensure_expression, pc
from arrowdsl.plan.joins import JoinOutputSpec, join_plan, join_spec
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import project_to_schema
from arrowdsl.schema.schema import empty_table
from normalize.registry_specs import (
    dataset_input_columns,
    dataset_input_schema,
    dataset_query,
    dataset_schema,
)
from normalize.utils import PlanSource, plan_source

DEF_USE_NAME = "py_bc_def_use_events_v1"
REACHES_NAME = "py_bc_reaches_v1"


def _to_plan(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
) -> Plan:
    return plan_source(source, ctx=ctx, columns=columns)


def def_use_events_plan(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane def/use event table.

    Returns
    -------
    Plan
        Plan producing def/use event rows.
    """
    base_names = dataset_input_columns(DEF_USE_NAME)
    plan = _to_plan(py_bc_instructions, ctx=ctx, columns=base_names)
    plan = project_to_schema(plan, schema=dataset_input_schema(DEF_USE_NAME), ctx=ctx)
    plan = dataset_query(DEF_USE_NAME).apply_to_plan(plan, ctx=ctx)
    valid = ensure_expression(
        pc.and_(pc.is_valid(pc.field("symbol")), pc.is_valid(pc.field("kind")))
    )
    return plan.filter(valid, ctx=ctx)


def _def_use_subset_plan(
    plan: Plan,
    *,
    kind: str,
    event_col: str,
    include_meta: bool,
    ctx: ExecutionContext,
) -> Plan:
    predicate = ensure_expression(pc.equal(pc.field("kind"), pc.scalar(kind)))
    plan = plan.filter(predicate, ctx=ctx)

    names = ["code_unit_id", "symbol", event_col]
    exprs = [pc.field("code_unit_id"), pc.field("symbol"), pc.field("event_id")]
    if include_meta:
        names.extend(["path", "file_id"])
        exprs.extend([pc.field("path"), pc.field("file_id")])
    return plan.project(exprs, names, ctx=ctx)


def reaching_defs_plan(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext,
) -> Plan:
    """Build a plan-lane reaching-defs edge table.

    Returns
    -------
    Plan
        Plan producing reaching-def edges.
    """
    plan = _to_plan(
        def_use_events,
        ctx=ctx,
        columns=["code_unit_id", "event_id", "kind", "symbol"],
    )
    available = set(plan.schema(ctx=ctx).names)
    required = {"kind", "code_unit_id", "symbol", "event_id"}
    if not required.issubset(available):
        return Plan.table_source(empty_table(dataset_schema(REACHES_NAME)))

    defs = _def_use_subset_plan(
        plan,
        kind="def",
        event_col="def_event_id",
        include_meta=False,
        ctx=ctx,
    )
    uses = _def_use_subset_plan(
        plan,
        kind="use",
        event_col="use_event_id",
        include_meta=True,
        ctx=ctx,
    )

    joined = join_plan(
        defs,
        uses,
        spec=join_spec(
            join_type="inner",
            left_keys=("code_unit_id", "symbol"),
            right_keys=("code_unit_id", "symbol"),
            output=JoinOutputSpec(
                left_output=("code_unit_id", "symbol", "def_event_id"),
                right_output=("use_event_id", "path", "file_id"),
            ),
        ),
        ctx=ctx,
    )

    if not isinstance(joined, Plan):
        joined = Plan.table_source(joined)
    return dataset_query(REACHES_NAME).apply_to_plan(joined, ctx=ctx)


__all__ = [
    "DEF_USE_NAME",
    "REACHES_NAME",
    "def_use_events_plan",
    "reaching_defs_plan",
]
