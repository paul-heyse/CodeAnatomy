"""Q-step execution helpers for `cq run`."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

from tools.cq.core.run_context import RunExecutionContext
from tools.cq.core.schema import CqResult
from tools.cq.query.batch import build_batch_session, filter_files_for_scope, select_files_by_rel
from tools.cq.query.batch_spans import collect_span_filters
from tools.cq.query.execution_requests import EntityQueryRequest, PatternQueryRequest
from tools.cq.query.executor import (
    execute_entity_query_from_records,
    execute_pattern_query_with_files,
)
from tools.cq.query.ir import Query
from tools.cq.query.language import QueryLanguage
from tools.cq.query.planner import ToolPlan
from tools.cq.query.sg_parser import list_scan_files
from tools.cq.run.helpers import error_result
from tools.cq.run.spec import QStep
from tools.cq.run.step_executors import RUN_STEP_NON_FATAL_EXCEPTIONS


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
            result = error_result(step.parent_step_id, "q", exc, ctx)
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
            result = error_result(step.parent_step_id, "q", exc, ctx)
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


__all__ = ["execute_entity_q_steps", "execute_pattern_q_steps"]
