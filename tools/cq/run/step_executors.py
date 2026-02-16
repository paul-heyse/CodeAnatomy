"""Step execution for non-Q run steps."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import msgspec

from tools.cq.core.run_context import RunExecutionContext
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import CqResult, Finding
from tools.cq.orchestration.request_factory import (
    RequestContextV1,
    RequestFactory,
    SearchRequestOptionsV1,
)
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE_SCOPE
from tools.cq.run.helpers import error_result as _error_result
from tools.cq.run.helpers import merge_in_dir as _merge_in_dir
from tools.cq.run.spec import (
    BytecodeSurfaceStep,
    CallsStep,
    ExceptionsStep,
    ImpactStep,
    ImportsStep,
    NeighborhoodStep,
    RunPlan,
    RunStep,
    ScopesStep,
    SearchStep,
    SideEffectsStep,
    SigImpactStep,
    step_type,
)
from tools.cq.search.pipeline.smart_search import SMART_SEARCH_LIMITS

RUN_STEP_NON_FATAL_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    ValueError,
    TypeError,
)
logger = logging.getLogger(__name__)


def execute_non_q_step(
    step: RunStep, plan: RunPlan, ctx: RunExecutionContext, *, run_id: str
) -> CqResult:
    """Execute a non-Q step.

    Parameters:
        step: Step to execute.
        plan: Parent run plan for scope filtering.
        ctx: CLI context.
        run_id: Run ID for tracing.

    Returns:
        CqResult: Step execution result.

    Raises:
        TypeError: When the step type has no registered executor.
    """
    logger.debug("Executing non-q step type=%s run_id=%s", step_type(step), run_id)
    if isinstance(step, SearchStep):
        return _execute_search_step(step, plan, ctx, run_id=run_id)
    if isinstance(step, NeighborhoodStep):
        return _execute_neighborhood_step(step, ctx, run_id=run_id)

    executor = _NON_SEARCH_DISPATCH.get(type(step))
    if executor is None:
        msg = f"Unsupported step type: {type(step)!r}"
        raise TypeError(msg)

    result = executor(step, ctx)
    return _apply_run_scope_filter(result, ctx.root, plan.in_dir, plan.exclude)


def execute_non_q_step_safe(
    step: RunStep,
    plan: RunPlan,
    ctx: RunExecutionContext,
    *,
    run_id: str,
) -> tuple[str, CqResult]:
    """Execute a non-Q step with exception handling.

    Parameters
    ----------
    step : RunStep
        Step to execute.
    plan : RunPlan
        Parent run plan for scope filtering.
    ctx : CliContext
        CLI context.
    run_id : str
        Run ID for tracing.

    Returns:
    -------
    tuple[str, CqResult]
        (step_id, result) tuple with error result on exception.
    """
    step_id = step.id or step_type(step)
    try:
        return step_id, execute_non_q_step(step, plan, ctx, run_id=run_id)
    except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
        logger.warning("Non-q step failed step_id=%s type=%s: %s", step_id, step_type(step), exc)
        return step_id, _error_result(step_id, step_type(step), exc, ctx)


def execute_non_q_steps_serial(
    steps: list[RunStep],
    plan: RunPlan,
    ctx: RunExecutionContext,
    *,
    run_id: str,
    stop_on_error: bool,
) -> list[tuple[str, CqResult]]:
    """Execute non-Q steps serially.

    Parameters
    ----------
    steps : list[RunStep]
        Steps to execute.
    plan : RunPlan
        Parent run plan for scope filtering.
    ctx : CliContext
        CLI context.
    run_id : str
        Run ID for tracing.
    stop_on_error : bool
        Stop on first error if True.

    Returns:
    -------
    list[tuple[str, CqResult]]
        List of (step_id, result) tuples.
    """
    results: list[tuple[str, CqResult]] = []
    for step in steps:
        step_id, result = execute_non_q_step_safe(step, plan, ctx, run_id=run_id)
        results.append((step_id, result))
        if stop_on_error and result.summary.error:
            break
    return results


def execute_non_q_steps_parallel(
    steps: list[RunStep],
    plan: RunPlan,
    ctx: RunExecutionContext,
    *,
    run_id: str,
) -> list[tuple[str, CqResult]]:
    """Execute non-Q steps in parallel.

    Parameters
    ----------
    steps : list[RunStep]
        Steps to execute.
    plan : RunPlan
        Parent run plan for scope filtering.
    ctx : CliContext
        CLI context.
    run_id : str
        Run ID for tracing.

    Returns:
    -------
    list[tuple[str, CqResult]]
        List of (step_id, result) tuples.
    """
    if len(steps) <= 1:
        return execute_non_q_steps_serial(steps, plan, ctx, run_id=run_id, stop_on_error=False)
    scheduler = get_worker_scheduler()
    if scheduler.policy.run_step_workers <= 1:
        return execute_non_q_steps_serial(steps, plan, ctx, run_id=run_id, stop_on_error=False)
    futures = [
        scheduler.submit_io(execute_non_q_step_safe, step, plan, ctx, run_id=run_id)
        for step in steps
    ]
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=max(1.0, float(len(steps)) * 60.0),
    )
    if batch.timed_out > 0:
        logger.warning(
            "Parallel non-q execution timed out for %d steps; falling back to serial",
            len(steps),
        )
        return execute_non_q_steps_serial(steps, plan, ctx, run_id=run_id, stop_on_error=False)
    return batch.done


def _execute_search_step(
    step: SearchStep,
    plan: RunPlan,
    ctx: RunExecutionContext,
    *,
    run_id: str,
) -> CqResult:
    mode = None
    if step.mode == "regex":
        from tools.cq.search._shared.types import QueryMode

        mode = QueryMode.REGEX
    elif step.mode == "literal":
        from tools.cq.search._shared.types import QueryMode

        mode = QueryMode.LITERAL

    include_globs = _build_search_includes(plan.in_dir, step.in_dir)
    exclude_globs = list(plan.exclude) if plan.exclude else None

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.search(
        request_ctx,
        query=step.query,
        options=SearchRequestOptionsV1(
            mode=mode,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            include_strings=step.include_strings,
            lang_scope=step.lang_scope,
            limits=SMART_SEARCH_LIMITS,
            run_id=run_id,
        ),
    )

    return ctx.services.search.execute(request)


def execute_search_fallback(query: str, plan: RunPlan, ctx: RunExecutionContext) -> CqResult:
    """Execute search step as fallback for unparseable Q queries.

    Parameters
    ----------
    query : str
        Query string.
    plan : RunPlan
        Parent run plan for scope filtering.
    ctx : CliContext
        CLI context.

    Returns:
    -------
    CqResult
        Search result.
    """
    include_globs = _build_search_includes(plan.in_dir, None)
    exclude_globs = list(plan.exclude) if plan.exclude else None

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.search(
        request_ctx,
        query=query,
        options=SearchRequestOptionsV1(
            mode=None,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            include_strings=False,
            lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
            limits=SMART_SEARCH_LIMITS,
        ),
    )

    return ctx.services.search.execute(request)


def _execute_calls(step: CallsStep, ctx: RunExecutionContext) -> CqResult:
    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.calls(request_ctx, function_name=step.function)

    return ctx.services.calls.execute(request)


def _execute_impact(step: ImpactStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.impact import cmd_impact

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.impact(
        request_ctx,
        function_name=step.function,
        param_name=step.param,
        max_depth=step.depth,
    )
    return cmd_impact(request)


def _execute_imports(step: ImportsStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.imports import cmd_imports

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.imports_cmd(request_ctx, cycles=step.cycles, module=step.module)
    return cmd_imports(request)


def _execute_exceptions(step: ExceptionsStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.exceptions import cmd_exceptions

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.exceptions(request_ctx, function=step.function)
    return cmd_exceptions(request)


def _execute_sig_impact(step: SigImpactStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.sig_impact import cmd_sig_impact

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.sig_impact(request_ctx, symbol=step.symbol, to=step.to)
    return cmd_sig_impact(request)


def _execute_side_effects(step: SideEffectsStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.side_effects import cmd_side_effects

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.side_effects(request_ctx, max_files=step.max_files)
    return cmd_side_effects(request)


def _execute_scopes(step: ScopesStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.scopes import cmd_scopes

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.scopes(request_ctx, target=step.target, max_files=step.max_files)
    return cmd_scopes(request)


def _execute_bytecode_surface(step: BytecodeSurfaceStep, ctx: RunExecutionContext) -> CqResult:
    from tools.cq.macros.bytecode import cmd_bytecode_surface

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.bytecode_surface(
        request_ctx,
        target=step.target,
        show=step.show,
        max_files=step.max_files,
    )
    return cmd_bytecode_surface(request)


def _execute_neighborhood_step(
    step: NeighborhoodStep,
    ctx: RunExecutionContext,
    *,
    run_id: str | None = None,
) -> CqResult:
    """Execute a neighborhood step.

    Parameters
    ----------
    step : NeighborhoodStep
        Neighborhood step configuration.
    ctx : CliContext
        CLI context.
    run_id : str | None
        Run ID for tracing.

    Returns:
    -------
    CqResult
        Neighborhood analysis result.
    """
    from tools.cq.neighborhood.executor import NeighborhoodExecutionRequest, execute_neighborhood

    return execute_neighborhood(
        NeighborhoodExecutionRequest(
            target=step.target,
            root=ctx.root,
            argv=ctx.argv,
            toolchain=ctx.toolchain,
            lang=step.lang,
            top_k=step.top_k,
            semantic_enrichment=step.semantic_enrichment,
            artifact_dir=ctx.artifact_dir,
            run_id=run_id,
            services=ctx.services,
        )
    )


def _apply_run_scope_filter(
    result: CqResult,
    root: Path,
    in_dir: str | None,
    exclude: tuple[str, ...],
) -> CqResult:
    if not in_dir and not exclude:
        return result

    base = (root / in_dir).resolve() if in_dir else None
    exclude_patterns = [pattern.lstrip("!") for pattern in exclude]

    def in_scope(finding: Finding) -> bool:
        if finding.anchor is None:
            return True
        rel_path = Path(finding.anchor.file)
        abs_path = (root / rel_path).resolve()
        if base and not abs_path.is_relative_to(base):
            return False
        if not exclude_patterns:
            return True
        return all(not rel_path.match(pattern) for pattern in exclude_patterns)

    key_findings = [f for f in result.key_findings if in_scope(f)]
    evidence = [f for f in result.evidence if in_scope(f)]
    sections = []
    for section in result.sections:
        section_findings = [f for f in section.findings if in_scope(f)]
        if not section_findings:
            continue
        sections.append(
            msgspec.structs.replace(section, findings=section_findings),
        )

    return CqResult(
        run=result.run,
        summary=result.summary,
        key_findings=key_findings,
        evidence=evidence,
        sections=sections,
        artifacts=result.artifacts,
    )


def _build_search_includes(run_in_dir: str | None, step_in_dir: str | None) -> list[str] | None:
    combined = _merge_in_dir(run_in_dir, step_in_dir)
    if not combined:
        return None
    return [f"{combined}/**"]


# Dispatch table for non-q, non-search step types.
# Defined after all executor functions to avoid forward reference issues.
_NON_SEARCH_DISPATCH: dict[type, Callable[..., CqResult]] = {
    CallsStep: _execute_calls,
    ImpactStep: _execute_impact,
    ImportsStep: _execute_imports,
    ExceptionsStep: _execute_exceptions,
    SigImpactStep: _execute_sig_impact,
    SideEffectsStep: _execute_side_effects,
    ScopesStep: _execute_scopes,
    BytecodeSurfaceStep: _execute_bytecode_surface,
    NeighborhoodStep: _execute_neighborhood_step,
}

__all__ = [
    "execute_non_q_step",
    "execute_non_q_step_safe",
    "execute_non_q_steps_parallel",
    "execute_non_q_steps_serial",
    "execute_search_fallback",
]
