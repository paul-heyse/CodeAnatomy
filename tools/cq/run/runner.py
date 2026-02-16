"""Run plan execution engine for cq run."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import msgspec

from tools.cq.cli_app.context import CliContext
from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.cache import maybe_evict_run_cache_tag
from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.contracts import MergeResultsRequest
from tools.cq.core.merge import merge_step_results
from tools.cq.core.multilang_orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.core.request_factory import (
    RequestContextV1,
    RequestFactory,
    SearchRequestOptionsV1,
)
from tools.cq.core.result_factory import build_error_result
from tools.cq.core.run_context import RunContext
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import CqResult, Finding, assign_result_finding_ids, mk_result, ms
from tools.cq.query.batch import build_batch_session, filter_files_for_scope, select_files_by_rel
from tools.cq.query.batch_spans import collect_span_filters
from tools.cq.query.execution_requests import (
    EntityQueryRequest,
    PatternQueryRequest,
)
from tools.cq.query.executor import (
    execute_entity_query_from_records,
    execute_pattern_query_with_files,
)
from tools.cq.query.ir import Query, Scope
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
)
from tools.cq.query.parser import QueryParseError, parse_query
from tools.cq.query.planner import ToolPlan, compile_query, scope_to_globs, scope_to_paths
from tools.cq.query.sg_parser import list_scan_files
from tools.cq.run.spec import (
    BytecodeSurfaceStep,
    CallsStep,
    ExceptionsStep,
    ImpactStep,
    ImportsStep,
    NeighborhoodStep,
    QStep,
    RunPlan,
    RunStep,
    ScopesStep,
    SearchStep,
    SideEffectsStep,
    SigImpactStep,
    normalize_step_ids,
    step_type,
)
from tools.cq.search.pipeline.smart_search import SMART_SEARCH_LIMITS
from tools.cq.search.semantic.diagnostics import (
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
from tools.cq.run.q_step_collapsing import collapse_parent_q_results
from tools.cq.run.run_summary import populate_run_summary_metadata
from tools.cq.run.step_executors import (
    RUN_STEP_NON_FATAL_EXCEPTIONS,
    execute_non_q_step_safe,
    execute_non_q_steps_parallel,
    execute_non_q_steps_serial,
    execute_search_fallback,
)
from tools.cq.utils.uuid_factory import uuid7_str


@dataclass(frozen=True)
class ParsedQStep:
    step_id: str
    parent_step_id: str
    step: QStep
    query: Query
    plan: ToolPlan
    scope_paths: list[Path]
    scope_globs: list[str] | None


def execute_run_plan(plan: RunPlan, ctx: CliContext, *, stop_on_error: bool = False) -> CqResult:
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
    merged = mk_result(run_ctx.to_runmeta("run"))
    merged.summary["plan_version"] = plan.version

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
    merged.summary["cache_backend"] = snapshot_backend_metrics(root=ctx.root)
    assign_result_finding_ids(merged)
    maybe_evict_run_cache_tag(root=ctx.root, language="python", run_id=run_id)
    maybe_evict_run_cache_tag(root=ctx.root, language="rust", run_id=run_id)
    return merged


def _execute_q_steps(
    steps: list[QStep],
    plan: RunPlan,
    ctx: CliContext,
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
    ctx: CliContext,
    stop_on_error: bool,
    immediate_results: list[tuple[str, CqResult]],
) -> tuple[dict[QueryLanguage, list[ParsedQStep]], dict[QueryLanguage, list[ParsedQStep]]]:
    parsed_by_lang: dict[QueryLanguage, list[ParsedQStep]] = {}
    pattern_by_lang: dict[QueryLanguage, list[ParsedQStep]] = {}

    for step in steps:
        step_id, outcome, is_error = _prepare_q_step(step, plan, ctx)
        if isinstance(outcome, CqResult):
            immediate_results.append((step_id, outcome))
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
    ctx: CliContext,
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
    return any("error" in result.summary for _, result in results)


def _prepare_q_step(
    step: QStep,
    plan: RunPlan,
    ctx: CliContext,
) -> tuple[str, ParsedQStep | CqResult, bool]:
    step_id = step.id or "q"
    try:
        query = parse_query(step.query)
    except QueryParseError as exc:
        return _handle_query_parse_error(step, step_id, plan, ctx, exc)

    query = _apply_run_scope(query, plan)
    tool_plan = compile_query(query)
    scope_paths = scope_to_paths(tool_plan.scope, ctx.root)
    if not scope_paths:
        msg = "No files match scope"
        return step_id, _error_result(step_id, "q", RuntimeError(msg), ctx), True
    scope_globs = scope_to_globs(tool_plan.scope)
    parsed_step = ParsedQStep(
        step_id=step_id,
        parent_step_id=step_id,
        step=step,
        query=query,
        plan=tool_plan,
        scope_paths=scope_paths,
        scope_globs=scope_globs,
    )
    return step_id, parsed_step, False


def _expand_q_step_by_scope(step: ParsedQStep, ctx: CliContext) -> list[ParsedQStep]:
    if step.query.lang_scope != "auto":
        return [step]

    expanded: list[ParsedQStep] = []
    for lang in expand_language_scope(step.query.lang_scope):
        scoped_query = msgspec.structs.replace(step.query, lang_scope=lang)
        scoped_plan = compile_query(scoped_query)
        scoped_paths = scope_to_paths(scoped_plan.scope, ctx.root)
        expanded.append(
            ParsedQStep(
                step_id=f"{step.parent_step_id}:{lang}",
                parent_step_id=step.parent_step_id,
                step=step.step,
                query=scoped_query,
                plan=scoped_plan,
                scope_paths=scoped_paths,
                scope_globs=scope_to_globs(scoped_plan.scope),
            )
        )
    return expanded


def _handle_query_parse_error(
    step: QStep,
    step_id: str,
    plan: RunPlan,
    ctx: CliContext,
    exc: QueryParseError,
) -> tuple[str, CqResult, bool]:
    if not _has_query_tokens(step.query):
        return step_id, execute_search_fallback(step.query, plan, ctx), False
    return step_id, _error_result(step_id, "q", exc, ctx), True


def _execute_entity_q_steps(
    steps: list[ParsedQStep],
    ctx: CliContext,
    *,
    stop_on_error: bool,
    run_id: str,
) -> list[tuple[str, CqResult]]:
    if not ctx.toolchain.has_sgpy:
        return [
            (
                step.parent_step_id,
                _error_result(
                    step.parent_step_id,
                    "q",
                    RuntimeError("ast-grep not available"),
                    ctx,
                ),
            )
            for step in steps
        ]

    union_paths = _unique_paths([path for step in steps for path in step.scope_paths])
    record_types = _union_record_types(steps)
    lang = steps[0].plan.lang if steps else "python"
    session = build_batch_session(
        root=ctx.root,
        tc=ctx.toolchain,
        paths=union_paths,
        record_types=record_types,
        lang=lang,
    )

    allowed_files = [
        filter_files_for_scope(session.files, ctx.root, step.plan.scope) for step in steps
    ]
    span_files_rel = _union_span_files(steps, allowed_files)
    span_files = select_files_by_rel(session.files_by_rel, span_files_rel)
    match_spans = collect_span_filters(
        root=ctx.root,
        files=span_files,
        queries=[step.query for step in steps],
        plans=[step.plan for step in steps],
    )

    results: list[tuple[str, CqResult]] = []
    for step, allowed, spans in zip(steps, allowed_files, match_spans, strict=True):
        records = [record for record in session.records if record.file in allowed]
        request = EntityQueryRequest(
            plan=step.plan,
            query=step.query,
            tc=ctx.toolchain,
            root=ctx.root,
            records=records,
            paths=step.scope_paths,
            scope_globs=step.scope_globs,
            argv=ctx.argv,
            run_id=run_id,
            query_text=step.step.query,
            match_spans=spans,
            symtable=session.symtable,
        )
        try:
            result = execute_entity_query_from_records(request)
        except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
            result = _error_result(step.parent_step_id, "q", exc, ctx)
            result.summary["lang"] = step.plan.lang
            result.summary["query_text"] = step.step.query
            results.append((step.parent_step_id, result))
            if stop_on_error:
                break
            continue
        result.summary["lang"] = step.plan.lang
        result.summary["query_text"] = step.step.query
        results.append((step.parent_step_id, result))
    return results


def _execute_pattern_q_steps(
    steps: list[ParsedQStep],
    ctx: CliContext,
    *,
    stop_on_error: bool,
    run_id: str,
) -> list[tuple[str, CqResult]]:
    if not ctx.toolchain.has_sgpy:
        return [
            (
                step.parent_step_id,
                _error_result(
                    step.parent_step_id,
                    "q",
                    RuntimeError("ast-grep not available"),
                    ctx,
                ),
            )
            for step in steps
        ]
    union_paths = _unique_paths([path for step in steps for path in step.scope_paths])
    lang = steps[0].plan.lang if steps else "python"
    pattern_files = _tabulate_files(ctx.root, union_paths, lang=lang)
    files_by_rel: dict[str, Path] = {}
    for path in pattern_files:
        try:
            rel = path.relative_to(ctx.root).as_posix()
        except ValueError:
            rel = path.as_posix()
        files_by_rel[rel] = path

    results: list[tuple[str, CqResult]] = []
    for step in steps:
        allowed_rel = filter_files_for_scope(pattern_files, ctx.root, step.plan.scope)
        files = select_files_by_rel(files_by_rel, allowed_rel)
        request = PatternQueryRequest(
            plan=step.plan,
            query=step.query,
            tc=ctx.toolchain,
            root=ctx.root,
            files=files,
            argv=ctx.argv,
            run_id=run_id,
            query_text=step.step.query,
        )
        try:
            result = execute_pattern_query_with_files(request)
        except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
            result = _error_result(step.parent_step_id, "q", exc, ctx)
            result.summary["lang"] = step.plan.lang
            result.summary["query_text"] = step.step.query
            results.append((step.parent_step_id, result))
            if stop_on_error:
                break
            continue
        result.summary["lang"] = step.plan.lang
        result.summary["query_text"] = step.step.query
        results.append((step.parent_step_id, result))
    return results


def _apply_run_scope(query: Query, plan: RunPlan) -> Query:
    if not plan.in_dir and not plan.exclude:
        return query
    scope = query.scope
    merged = Scope(
        in_dir=_merge_in_dir(plan.in_dir, scope.in_dir),
        exclude=tuple(_merge_excludes(plan.exclude, scope.exclude)),
        globs=scope.globs,
    )
    return query.with_scope(merged)


def _merge_in_dir(run_in_dir: str | None, step_in_dir: str | None) -> str | None:
    if run_in_dir and step_in_dir:
        return str(Path(run_in_dir) / step_in_dir)
    return step_in_dir or run_in_dir


def _merge_excludes(
    run_exclude: Iterable[str],
    step_exclude: Iterable[str],
) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for item in list(run_exclude) + list(step_exclude):
        if item in seen:
            continue
        seen.add(item)
        merged.append(item)
    return merged


def _union_record_types(steps: list[ParsedQStep]) -> set[str]:
    record_types: set[str] = set()
    for step in steps:
        record_types |= set(step.plan.sg_record_types)
    return record_types


def _union_span_files(steps: list[ParsedQStep], allowed: list[set[str]]) -> set[str]:
    rel_paths: set[str] = set()
    for step, allowed_files in zip(steps, allowed, strict=True):
        if not step.plan.sg_rules:
            continue
        rel_paths.update(allowed_files)
    return rel_paths


def _unique_paths(paths: Iterable[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def _tabulate_files(
    root: Path,
    paths: list[Path],
    *,
    lang: QueryLanguage,
) -> list[Path]:
    return list_scan_files(
        paths=paths,
        root=root,
        globs=None,
        lang=lang,
    )




def _has_query_tokens(query_string: str) -> bool:
    token_pattern = r"([\\w.]+|\\$+\\w+)=(?:'([^']+)'|\\\"([^\\\"]+)\\\"|([^\\s]+))"
    return bool(list(re.finditer(token_pattern, query_string)))


def _error_result(step_id: str, macro: str, exc: Exception, ctx: CliContext) -> CqResult:
    result = build_error_result(
        macro=macro,
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
        started_ms=ms(),
        error=exc,
    )
    result.key_findings.append(
        Finding(category="error", message=f"{step_id}: {exc}", severity="error")
    )
    return assign_result_finding_ids(result)


__all__ = [
    "execute_run_plan",
]
