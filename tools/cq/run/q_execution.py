"""Q-step execution helpers for `cq run`."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import msgspec

from tools.cq.core.run_context import RunExecutionContext
from tools.cq.core.schema import CqResult, update_result_summary
from tools.cq.core.types import QueryLanguage
from tools.cq.query.batch import build_batch_session, filter_files_for_scope, select_files_by_rel
from tools.cq.query.batch_spans import collect_span_filters
from tools.cq.query.execution_requests import EntityQueryRequest, PatternQueryRequest
from tools.cq.query.executor_runtime_entity import execute_entity_query_from_records
from tools.cq.query.executor_runtime_pattern import execute_pattern_query_with_files
from tools.cq.query.ir import Query
from tools.cq.query.parser import QueryParseError, has_query_tokens, parse_query
from tools.cq.query.planner import ToolPlan, compile_query, scope_to_globs, scope_to_paths
from tools.cq.query.sg_parser import list_scan_files
from tools.cq.run.helpers import error_result
from tools.cq.run.scope import apply_run_scope
from tools.cq.run.spec import QStep, RunPlan
from tools.cq.run.step_executors import RUN_STEP_NON_FATAL_EXCEPTIONS, execute_search_fallback

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedQStep:
    """Parsed + planned q-step ready for grouped execution."""

    step_id: str
    parent_step_id: str
    step: QStep
    query: Query
    plan: ToolPlan
    scope_paths: list[Path]
    scope_globs: list[str] | None


class ParsedQStepLike(Protocol):
    """Protocol for parsed q-step payloads used by run execution."""

    @property
    def parent_step_id(self) -> str: ...

    @property
    def step(self) -> QStep: ...

    @property
    def query(self) -> Query: ...

    @property
    def plan(self) -> ToolPlan: ...

    @property
    def scope_paths(self) -> list[Path]: ...

    @property
    def scope_globs(self) -> list[str] | None: ...


def prepare_q_step(
    step: QStep,
    plan: RunPlan,
    ctx: RunExecutionContext,
) -> tuple[str, ParsedQStep | CqResult, bool]:
    """Parse + plan one q step.

    Returns:
        ``(step_id, parsed_or_error, is_error)``.
    """
    step_id = step.id or "q"
    try:
        query = parse_query(step.query)
    except QueryParseError as exc:
        return _handle_query_parse_error(step, step_id, plan, ctx, exc)

    query = apply_run_scope(query, plan.in_dir, plan.exclude)
    tool_plan = compile_query(query)
    scope_paths = scope_to_paths(tool_plan.scope, ctx.root)
    if not scope_paths:
        msg = "No files match scope"
        return step_id, error_result(step_id, "q", RuntimeError(msg), ctx), True
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


def expand_q_step_by_scope(step: ParsedQStep, ctx: RunExecutionContext) -> list[ParsedQStep]:
    """Expand ``lang_scope=auto`` into concrete per-language q-steps.

    Returns:
        list[ParsedQStep]: One parsed q-step per concrete language scope.
    """
    if step.query.lang_scope != "auto":
        return [step]

    expanded: list[ParsedQStep] = []
    for lang in ("python", "rust"):
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


def partition_q_steps(
    *,
    steps: list[QStep],
    plan: RunPlan,
    ctx: RunExecutionContext,
    stop_on_error: bool,
    immediate_results: list[tuple[str, CqResult]],
) -> tuple[dict[QueryLanguage, list[ParsedQStep]], dict[QueryLanguage, list[ParsedQStep]]]:
    """Partition q-steps into entity vs pattern groups keyed by language.

    Returns:
        tuple[dict[QueryLanguage, list[ParsedQStep]], dict[QueryLanguage, list[ParsedQStep]]]:
        Entity and pattern grouped q-steps.
    """
    parsed_by_lang: dict[QueryLanguage, list[ParsedQStep]] = {}
    pattern_by_lang: dict[QueryLanguage, list[ParsedQStep]] = {}

    for step in steps:
        step_id, outcome, is_error = prepare_q_step(step, plan, ctx)
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
        expanded = expand_q_step_by_scope(outcome, ctx)
        for parsed in expanded:
            target = pattern_by_lang if parsed.plan.is_pattern_query else parsed_by_lang
            target.setdefault(parsed.plan.lang, []).append(parsed)
    return parsed_by_lang, pattern_by_lang


def _handle_query_parse_error(
    step: QStep,
    step_id: str,
    plan: RunPlan,
    ctx: RunExecutionContext,
    exc: QueryParseError,
) -> tuple[str, CqResult, bool]:
    if not has_query_tokens(step.query):
        return step_id, execute_search_fallback(step.query, plan, ctx), False
    return step_id, error_result(step_id, "q", exc, ctx), True


def execute_entity_q_steps(
    steps: list[ParsedQStepLike],
    ctx: RunExecutionContext,
    *,
    stop_on_error: bool,
    run_id: str,
) -> list[tuple[str, CqResult]]:
    """Execute entity-style q steps in a shared batch session.

    Returns:
        list[tuple[str, CqResult]]: Step-id/result pairs for executed entity steps.
    """
    if not ctx.toolchain.has_sgpy:
        return [
            (
                step.parent_step_id,
                error_result(
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
        symtable=ctx.symtable_enricher,
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
            services=ctx.services,
            run_id=run_id,
            query_text=step.step.query,
            match_spans=spans,
            symtable=session.symtable,
        )
        try:
            result = execute_entity_query_from_records(request)
        except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
            result = error_result(step.parent_step_id, "q", exc, ctx)
            result = update_result_summary(
                result,
                {"lang": step.plan.lang, "query_text": step.step.query},
            )
            results.append((step.parent_step_id, result))
            if stop_on_error:
                break
            continue
        result = update_result_summary(
            result,
            {"lang": step.plan.lang, "query_text": step.step.query},
        )
        results.append((step.parent_step_id, result))
    return results


def execute_pattern_q_steps(
    steps: list[ParsedQStepLike],
    ctx: RunExecutionContext,
    *,
    stop_on_error: bool,
    run_id: str,
) -> list[tuple[str, CqResult]]:
    """Execute pattern-style q steps using scoped files per step.

    Returns:
        list[tuple[str, CqResult]]: Step-id/result pairs for executed pattern steps.
    """
    if not ctx.toolchain.has_sgpy:
        return [
            (
                step.parent_step_id,
                error_result(
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
    symtable = ctx.symtable_enricher
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
            services=ctx.services,
            symtable=symtable,
            run_id=run_id,
            query_text=step.step.query,
        )
        try:
            result = execute_pattern_query_with_files(request)
        except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
            result = error_result(step.parent_step_id, "q", exc, ctx)
            result = update_result_summary(
                result,
                {"lang": step.plan.lang, "query_text": step.step.query},
            )
            results.append((step.parent_step_id, result))
            if stop_on_error:
                break
            continue
        result = update_result_summary(
            result,
            {"lang": step.plan.lang, "query_text": step.step.query},
        )
        results.append((step.parent_step_id, result))
    return results


def _union_record_types(steps: list[ParsedQStepLike]) -> set[str]:
    record_types: set[str] = set()
    for step in steps:
        record_types |= set(step.plan.sg_record_types)
    return record_types


def _union_span_files(steps: list[ParsedQStepLike], allowed: list[set[str]]) -> set[str]:
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


__all__ = [
    "ParsedQStep",
    "execute_entity_q_steps",
    "execute_pattern_q_steps",
    "expand_q_step_by_scope",
    "partition_q_steps",
    "prepare_q_step",
]
