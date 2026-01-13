"""Derive bytecode def/use events and reaching-def edges."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, ensure_expression, pc
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.join_specs import JoinOutputSpec, join_spec
from arrowdsl.plan.joins import join_plan
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import project_to_schema
from arrowdsl.schema.schema import empty_table
from normalize.plan_helpers import PlanSource, plan_source
from normalize.runner import (
    ensure_canonical,
    ensure_execution_context,
    run_normalize,
    run_normalize_streamable_contract,
)
from normalize.schemas import (
    DEF_USE_CONTRACT,
    DEF_USE_QUERY,
    DEF_USE_SPEC,
    REACHES_CONTRACT,
    REACHES_QUERY,
    REACHES_SCHEMA,
    REACHES_SPEC,
)

_EVENT_INPUT_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("file_id", pa.string()),
    ("path", pa.string()),
    ("instr_id", pa.string()),
    ("code_unit_id", pa.string()),
    ("opname", pa.string()),
    ("offset", pa.int32()),
    ("argval_str", pa.string()),
    ("argrepr", pa.string()),
)
_EVENT_INPUT_SCHEMA = pa.schema([pa.field(name, dtype) for name, dtype in _EVENT_INPUT_COLUMNS])


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
    base_names = [name for name, _ in _EVENT_INPUT_COLUMNS]
    plan = _to_plan(py_bc_instructions, ctx=ctx, columns=base_names)
    plan = project_to_schema(plan, schema=_EVENT_INPUT_SCHEMA, ctx=ctx)
    plan = DEF_USE_QUERY.apply_to_plan(plan, ctx=ctx)
    valid = ensure_expression(
        pc.and_(pc.is_valid(pc.field("symbol")), pc.is_valid(pc.field("kind")))
    )
    return plan.filter(valid, ctx=ctx)


def build_def_use_events_result(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Build def/use events from bytecode instruction rows.

    Parameters
    ----------
    py_bc_instructions:
        Bytecode instruction table with opname and argval data.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with def/use events.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = def_use_events_plan(py_bc_instructions, ctx=exec_ctx)
    return run_normalize(
        plan=plan,
        post=(),
        contract=DEF_USE_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=DEF_USE_SPEC.metadata_spec,
    )


def build_def_use_events(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Build def/use events from bytecode instruction rows.

    Parameters
    ----------
    py_bc_instructions:
        Bytecode instruction table with opname and argval data.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Def/use events table.
    """
    return build_def_use_events_result(py_bc_instructions, ctx=ctx).good


def build_def_use_events_canonical(
    py_bc_instructions: TableLike,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Build def/use events under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized def/use events table.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return build_def_use_events_result(py_bc_instructions, ctx=exec_ctx).good


def build_def_use_events_streamable(
    py_bc_instructions: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Build def/use events with a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = def_use_events_plan(py_bc_instructions, ctx=exec_ctx)
    return run_normalize_streamable_contract(plan, contract=DEF_USE_CONTRACT, ctx=exec_ctx)


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
    plan = _to_plan(def_use_events, ctx=ctx, columns=["code_unit_id", "event_id", "kind", "symbol"])
    available = set(plan.schema(ctx=ctx).names)
    required = {"kind", "code_unit_id", "symbol", "event_id"}
    if not required.issubset(available):
        return Plan.table_source(empty_table(REACHES_SCHEMA))

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
    return REACHES_QUERY.apply_to_plan(joined, ctx=ctx)


def run_reaching_defs_result(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> FinalizeResult:
    """Compute a best-effort reaching-defs edge table.

    This is a conservative, symbol-matching approximation that joins definitions to uses
    within the same code unit. It is deterministic and safe for early-stage analysis.

    Parameters
    ----------
    def_use_events:
        Def/use events table.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    FinalizeResult
        Finalize bundle with reaching-def edges.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = reaching_defs_plan(def_use_events, ctx=exec_ctx)
    return run_normalize(
        plan=plan,
        post=(),
        contract=REACHES_CONTRACT,
        ctx=exec_ctx,
        metadata_spec=REACHES_SPEC.metadata_spec,
    )


def run_reaching_defs(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Compute a best-effort reaching-defs edge table.

    This is a conservative, symbol-matching approximation that joins definitions to uses
    within the same code unit. It is deterministic and safe for early-stage analysis.

    Parameters
    ----------
    def_use_events:
        Def/use events table.
    ctx:
        Optional execution context for plan compilation and finalize.

    Returns
    -------
    TableLike
        Reaching-def edges table.
    """
    return run_reaching_defs_result(def_use_events, ctx=ctx).good


def run_reaching_defs_canonical(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike:
    """Compute reaching-def edges under canonical determinism.

    Returns
    -------
    TableLike
        Canonicalized reaching-def edges.
    """
    exec_ctx = ensure_canonical(ensure_execution_context(ctx))
    return run_reaching_defs_result(def_use_events, ctx=exec_ctx).good


def run_reaching_defs_streamable(
    def_use_events: PlanSource,
    *,
    ctx: ExecutionContext | None = None,
) -> TableLike | RecordBatchReaderLike:
    """Compute reaching-def edges with a streamable output.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Reader when streamable, otherwise a materialized table.
    """
    exec_ctx = ensure_execution_context(ctx)
    plan = reaching_defs_plan(def_use_events, ctx=exec_ctx)
    return run_normalize_streamable_contract(plan, contract=REACHES_CONTRACT, ctx=exec_ctx)


__all__ = [
    "build_def_use_events",
    "build_def_use_events_canonical",
    "build_def_use_events_result",
    "build_def_use_events_streamable",
    "def_use_events_plan",
    "reaching_defs_plan",
    "run_reaching_defs",
    "run_reaching_defs_canonical",
    "run_reaching_defs_result",
    "run_reaching_defs_streamable",
]
