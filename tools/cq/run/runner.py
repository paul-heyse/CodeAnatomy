"""Run plan execution engine for cq run."""

from __future__ import annotations

import logging
from collections.abc import Callable

from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.run_lifecycle import maybe_evict_run_cache_tag
from tools.cq.core.merge import merge_step_results
from tools.cq.core.run_context import RunContext, RunExecutionContext
from tools.cq.core.schema import CqResult, assign_result_finding_ids, mk_result, ms
from tools.cq.query.language import QueryLanguage
from tools.cq.run.q_execution import (
    ParsedQStep,
)
from tools.cq.run.q_execution import (
    execute_entity_q_steps as _execute_entity_q_steps,
)
from tools.cq.run.q_execution import (
    execute_pattern_q_steps as _execute_pattern_q_steps,
)
from tools.cq.run.q_execution import (
    expand_q_step_by_scope as _expand_q_step_by_scope,
)
from tools.cq.run.q_execution import (
    prepare_q_step as _prepare_q_step,
)
from tools.cq.run.q_step_collapsing import collapse_parent_q_results
from tools.cq.run.run_summary import populate_run_summary_metadata
from tools.cq.run.spec import (
    QStep,
    RunPlan,
    RunStep,
    normalize_step_ids,
)
from tools.cq.run.step_executors import (
    execute_non_q_steps_parallel,
    execute_non_q_steps_serial,
)
from tools.cq.utils.uuid_factory import uuid7_str

logger = logging.getLogger(__name__)


def execute_run_plan(
    plan: RunPlan, ctx: RunExecutionContext, *, stop_on_error: bool = False
) -> CqResult:
    """Execute a RunPlan and return the merged CQ result.

    Returns:
    -------
    CqResult
        Aggregated run result for all steps.
    """
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
        started_ms=ms(),
    )
    run_id = run_ctx.run_id or uuid7_str()
    logger.debug(
        "Executing run plan steps=%d stop_on_error=%s root=%s",
        len(plan.steps),
        stop_on_error,
        ctx.root,
    )
    merged = mk_result(run_ctx.to_runmeta("run"))
    merged.summary.plan_version = plan.version

    steps = normalize_step_ids(plan.steps)
    executed_results: list[tuple[str, CqResult]] = []
    q_steps: list[QStep] = []
    other_steps: list[RunStep] = []
    for step in steps:
        if isinstance(step, QStep):
            q_steps.append(step)
        else:
            other_steps.append(step)

    for step_id, result in _execute_q_steps(
        q_steps,
        plan,
        ctx,
        stop_on_error=stop_on_error,
        run_id=run_id,
    ):
        merge_step_results(merged, step_id, result)
        executed_results.append((step_id, result))

    non_q_results = (
        execute_non_q_steps_serial(
            other_steps,
            plan,
            ctx,
            run_id=run_id,
            stop_on_error=stop_on_error,
        )
        if stop_on_error
        else execute_non_q_steps_parallel(other_steps, plan, ctx, run_id=run_id)
    )
    for step_id, result in non_q_results:
        merge_step_results(merged, step_id, result)
        executed_results.append((step_id, result))

    populate_run_summary_metadata(merged, executed_results, total_steps=len(steps))
    merged.summary.cache_backend = snapshot_backend_metrics(root=ctx.root)
    merged = assign_result_finding_ids(merged)
    maybe_evict_run_cache_tag(root=ctx.root, language="python", run_id=run_id)
    maybe_evict_run_cache_tag(root=ctx.root, language="rust", run_id=run_id)
    logger.debug("Run plan completed steps=%d run_id=%s", len(steps), run_id)
    return merged


def _execute_q_steps(
    steps: list[QStep],
    plan: RunPlan,
    ctx: RunExecutionContext,
    *,
    stop_on_error: bool,
    run_id: str,
) -> list[tuple[str, CqResult]]:
    results: list[tuple[str, CqResult]] = []
    if not steps:
        return results

    parsed_by_lang, pattern_by_lang = _partition_q_steps(
        steps=steps,
        plan=plan,
        ctx=ctx,
        stop_on_error=stop_on_error,
        immediate_results=results,
    )
    if stop_on_error and _results_have_error(results):
        return results

    batch_specs: tuple[
        tuple[
            dict[QueryLanguage, list[ParsedQStep]],
            Callable[..., list[tuple[str, CqResult]]],
        ],
        ...,
    ] = (
        (parsed_by_lang, _execute_entity_q_steps),
        (pattern_by_lang, _execute_pattern_q_steps),
    )
    for grouped_steps, runner in batch_specs:
        should_stop = _run_grouped_q_batches(
            grouped_steps=grouped_steps,
            runner=runner,
            ctx=ctx,
            stop_on_error=stop_on_error,
            results=results,
            run_id=run_id,
        )
        if should_stop:
            return collapse_parent_q_results(results, ctx=ctx)
    return collapse_parent_q_results(results, ctx=ctx)


def _partition_q_steps(
    *,
    steps: list[QStep],
    plan: RunPlan,
    ctx: RunExecutionContext,
    stop_on_error: bool,
    immediate_results: list[tuple[str, CqResult]],
) -> tuple[dict[QueryLanguage, list[ParsedQStep]], dict[QueryLanguage, list[ParsedQStep]]]:
    parsed_by_lang: dict[QueryLanguage, list[ParsedQStep]] = {}
    pattern_by_lang: dict[QueryLanguage, list[ParsedQStep]] = {}

    for step in steps:
        step_id, outcome, is_error = _prepare_q_step(step, plan, ctx)
        if isinstance(outcome, CqResult):
            immediate_results.append((step_id, outcome))
            if outcome.summary.error:
                logger.warning(
                    "Immediate q-step error step_id=%s error=%s",
                    step_id,
                    outcome.summary.error,
                )
            if stop_on_error and is_error:
                break
            continue
        expanded = _expand_q_step_by_scope(outcome, ctx)
        for parsed in expanded:
            target = pattern_by_lang if parsed.plan.is_pattern_query else parsed_by_lang
            target.setdefault(parsed.plan.lang, []).append(parsed)
    return parsed_by_lang, pattern_by_lang


def _run_grouped_q_batches(
    *,
    grouped_steps: dict[QueryLanguage, list[ParsedQStep]],
    runner: Callable[..., list[tuple[str, CqResult]]],
    ctx: RunExecutionContext,
    stop_on_error: bool,
    results: list[tuple[str, CqResult]],
    run_id: str,
) -> bool:
    for parsed_steps in grouped_steps.values():
        batch_results = runner(parsed_steps, ctx, stop_on_error=stop_on_error, run_id=run_id)
        results.extend(batch_results)
        if stop_on_error and _results_have_error(batch_results):
            return True
    return False


def _results_have_error(results: list[tuple[str, CqResult]]) -> bool:
    return any(bool(result.summary.error) for _, result in results)


__all__ = [
    "execute_run_plan",
]
