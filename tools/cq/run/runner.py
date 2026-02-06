"""Run plan execution engine for cq run."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import msgspec

from tools.cq.cli_app.context import CliContext
from tools.cq.core.merge import merge_step_results
from tools.cq.core.multilang_orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, Finding, mk_result, ms
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
    file_extensions_for_scope,
)
from tools.cq.query.parser import QueryParseError, parse_query
from tools.cq.query.planner import ToolPlan, compile_query, scope_to_globs, scope_to_paths
from tools.cq.run.spec import (
    BytecodeSurfaceStep,
    CallsStep,
    ExceptionsStep,
    ImpactStep,
    ImportsStep,
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
from tools.cq.search.multilang_diagnostics import (
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    is_python_oriented_query_text,
)
from tools.cq.search.smart_search import SMART_SEARCH_LIMITS, smart_search


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
    merged = mk_result(run_ctx.to_runmeta("run"))
    merged.summary["plan_version"] = plan.version

    steps = normalize_step_ids(plan.steps)
    q_steps: list[QStep] = []
    other_steps: list[RunStep] = []
    for step in steps:
        if isinstance(step, QStep):
            q_steps.append(step)
        else:
            other_steps.append(step)

    for step_id, result in _execute_q_steps(q_steps, plan, ctx, stop_on_error=stop_on_error):
        merge_step_results(merged, step_id, result)

    for step in other_steps:
        step_id = step.id or step_type(step)
        try:
            result = _execute_non_q_step(step, plan, ctx)
        except Exception as exc:  # noqa: BLE001 - deliberate boundary
            result = _error_result(step_id, step_type(step), exc, ctx)
            merge_step_results(merged, step_id, result)
            if stop_on_error:
                break
            continue
        merge_step_results(merged, step_id, result)

    return merged


def _execute_q_steps(
    steps: list[QStep],
    plan: RunPlan,
    ctx: CliContext,
    *,
    stop_on_error: bool,
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
        )
        if should_stop:
            return _collapse_parent_q_results(results, ctx=ctx)
    return _collapse_parent_q_results(results, ctx=ctx)


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
) -> bool:
    for parsed_steps in grouped_steps.values():
        batch_results = runner(parsed_steps, ctx, stop_on_error=stop_on_error)
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
        return step_id, _execute_search_fallback(step.query, plan, ctx), False
    return step_id, _error_result(step_id, "q", exc, ctx), True


def _execute_entity_q_steps(
    steps: list[ParsedQStep],
    ctx: CliContext,
    *,
    stop_on_error: bool,
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
            match_spans=spans,
            symtable=session.symtable,
        )
        try:
            result = execute_entity_query_from_records(request)
        except Exception as exc:  # noqa: BLE001 - defensive boundary
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
    pattern_files = _tabulate_files(ctx.root, union_paths, lang_scope=lang)
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
        )
        try:
            result = execute_pattern_query_with_files(request)
        except Exception as exc:  # noqa: BLE001 - defensive boundary
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


def _result_language(result: CqResult) -> QueryLanguage | None:
    value = result.summary.get("lang")
    if value in {"python", "rust"}:
        return cast("QueryLanguage", value)
    return None


def _result_match_count(result: CqResult | None) -> int:
    if result is None:
        return 0
    total = result.summary.get("total_matches")
    if isinstance(total, int):
        return total
    matches = result.summary.get("matches")
    if isinstance(matches, int):
        return matches
    return len(result.key_findings)


def _build_collapse_diagnostics(
    lang_results: dict[QueryLanguage, CqResult],
    query_text: str,
) -> list[Finding]:
    """Build cross-language and capability diagnostics for collapsed results.

    Returns:
        Combined list of cross-language hint and capability limitation findings.
    """
    diagnostics: list[Finding] = list(
        build_cross_language_diagnostics(
            lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
            python_matches=_result_match_count(lang_results.get("python")),
            rust_matches=_result_match_count(lang_results.get("rust")),
            python_oriented=is_python_oriented_query_text(query_text),
        )
    )
    cap_features: list[str] = []
    if query_text:
        cap_features.append("pattern_query")
    diagnostics.extend(
        build_capability_diagnostics(
            features=cap_features,
            lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
        )
    )
    return diagnostics


def _collapse_parent_q_results(
    step_results: list[tuple[str, CqResult]],
    *,
    ctx: CliContext,
) -> list[tuple[str, CqResult]]:
    grouped: dict[str, list[CqResult]] = {}
    order: list[str] = []
    for step_id, result in step_results:
        if step_id not in grouped:
            grouped[step_id] = []
            order.append(step_id)
        grouped[step_id].append(result)

    collapsed: list[tuple[str, CqResult]] = []
    for step_id in order:
        group = grouped[step_id]
        if len(group) == 1:
            group[0].summary.pop("lang", None)
            group[0].summary.pop("query_text", None)
            collapsed.append((step_id, group[0]))
            continue

        lang_results: dict[QueryLanguage, CqResult] = {}
        query_text = ""
        for result in group:
            lang = _result_language(result)
            if lang is None:
                continue
            if lang not in lang_results:
                lang_results[lang] = result
            if not query_text:
                candidate = result.summary.get("query_text")
                if isinstance(candidate, str):
                    query_text = candidate

        diagnostics = _build_collapse_diagnostics(lang_results, query_text)
        run = runmeta_for_scope_merge(
            macro="q",
            root=ctx.root,
            argv=ctx.argv,
            tc=ctx.toolchain,
        )
        merged = merge_language_cq_results(
            scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
            results=lang_results,
            run=run,
            diagnostics=diagnostics,
        )
        collapsed.append((step_id, merged))
    return collapsed


def _execute_non_q_step(step: RunStep, plan: RunPlan, ctx: CliContext) -> CqResult:
    if isinstance(step, SearchStep):
        return _execute_search_step(step, plan, ctx)
    if isinstance(step, CallsStep):
        result = _execute_calls(step, ctx)
    elif isinstance(step, ImpactStep):
        result = _execute_impact(step, ctx)
    elif isinstance(step, ImportsStep):
        result = _execute_imports(step, ctx)
    elif isinstance(step, ExceptionsStep):
        result = _execute_exceptions(step, ctx)
    elif isinstance(step, SigImpactStep):
        result = _execute_sig_impact(step, ctx)
    elif isinstance(step, SideEffectsStep):
        result = _execute_side_effects(step, ctx)
    elif isinstance(step, ScopesStep):
        result = _execute_scopes(step, ctx)
    elif isinstance(step, BytecodeSurfaceStep):
        result = _execute_bytecode_surface(step, ctx)
    else:
        msg = f"Unsupported step type: {type(step)!r}"
        raise TypeError(msg)

    return _apply_run_scope_filter(result, ctx.root, plan.in_dir, plan.exclude)


def _execute_search_step(step: SearchStep, plan: RunPlan, ctx: CliContext) -> CqResult:
    if step.regex and step.literal:
        msg = "search step cannot set both regex and literal"
        raise RuntimeError(msg)

    mode = None
    if step.regex:
        from tools.cq.search.classifier import QueryMode

        mode = QueryMode.REGEX
    elif step.literal:
        from tools.cq.search.classifier import QueryMode

        mode = QueryMode.LITERAL

    include_globs = _build_search_includes(plan.in_dir, step.in_dir)
    exclude_globs = list(plan.exclude) if plan.exclude else None

    return smart_search(
        ctx.root,
        step.query,
        mode=mode,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        include_strings=step.include_strings,
        lang_scope=step.lang_scope,
        limits=SMART_SEARCH_LIMITS,
        tc=ctx.toolchain,
        argv=ctx.argv,
    )


def _execute_search_fallback(query: str, plan: RunPlan, ctx: CliContext) -> CqResult:
    include_globs = _build_search_includes(plan.in_dir, None)
    exclude_globs = list(plan.exclude) if plan.exclude else None
    return smart_search(
        ctx.root,
        query,
        mode=None,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        include_strings=False,
        lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
        limits=SMART_SEARCH_LIMITS,
        tc=ctx.toolchain,
        argv=ctx.argv,
    )


def _execute_calls(step: CallsStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.calls import cmd_calls

    return cmd_calls(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=step.function,
    )


def _execute_impact(step: ImpactStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=step.function,
        param_name=step.param,
        max_depth=step.depth,
    )
    return cmd_impact(request)


def _execute_imports(step: ImportsStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.imports import ImportRequest, cmd_imports

    request = ImportRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        cycles=step.cycles,
        module=step.module,
    )
    return cmd_imports(request)


def _execute_exceptions(step: ExceptionsStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.exceptions import cmd_exceptions

    return cmd_exceptions(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function=step.function,
    )


def _execute_sig_impact(step: SigImpactStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.sig_impact import SigImpactRequest, cmd_sig_impact

    request = SigImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        symbol=step.symbol,
        to=step.to,
    )
    return cmd_sig_impact(request)


def _execute_side_effects(step: SideEffectsStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.side_effects import SideEffectsRequest, cmd_side_effects

    request = SideEffectsRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        max_files=step.max_files,
    )
    return cmd_side_effects(request)


def _execute_scopes(step: ScopesStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.scopes import ScopeRequest, cmd_scopes

    request = ScopeRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=step.target,
        max_files=step.max_files,
    )
    return cmd_scopes(request)


def _execute_bytecode_surface(step: BytecodeSurfaceStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.bytecode import BytecodeSurfaceRequest, cmd_bytecode_surface

    request = BytecodeSurfaceRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        target=step.target,
        show=step.show,
        max_files=step.max_files,
    )
    return cmd_bytecode_surface(request)


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
    lang_scope: QueryLanguageScope,
) -> list[Path]:
    from tools.cq.index.files import build_repo_file_index, tabulate_files
    from tools.cq.index.repo import resolve_repo_context

    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    result = tabulate_files(
        repo_index,
        paths,
        None,
        extensions=file_extensions_for_scope(lang_scope),
    )
    return result.files


def _error_result(step_id: str, macro: str, exc: Exception, ctx: CliContext) -> CqResult:
    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
        started_ms=ms(),
    )
    result = mk_result(run_ctx.to_runmeta(macro))
    result.summary["error"] = str(exc)
    result.key_findings.append(
        Finding(category="error", message=f"{step_id}: {exc}", severity="error")
    )
    return result


def _has_query_tokens(query_string: str) -> bool:
    token_pattern = r"([\\w.]+|\\$+\\w+)=(?:'([^']+)'|\\\"([^\\\"]+)\\\"|([^\\s]+))"
    return bool(list(re.finditer(token_pattern, query_string)))


__all__ = [
    "execute_run_plan",
]
