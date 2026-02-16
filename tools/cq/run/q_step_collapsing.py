"""Q-step result collapsing and merging."""

from __future__ import annotations

from typing import cast

from tools.cq.core.contracts import MergeResultsRequest
from tools.cq.core.run_context import RunExecutionContext
from tools.cq.core.schema import CqResult, Finding
from tools.cq.orchestration.multilang_orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
)
from tools.cq.search.semantic.diagnostics import (
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)


def collapse_parent_q_results(
    step_results: list[tuple[str, CqResult]],
    *,
    ctx: RunExecutionContext,
) -> list[tuple[str, CqResult]]:
    """Collapse multi-language Q-step results by parent step ID.

    Parameters
    ----------
    step_results : list[tuple[str, CqResult]]
        List of (step_id, result) tuples.
    ctx : RunExecutionContext
        CLI context.

    Returns:
    -------
    list[tuple[str, CqResult]]
        Collapsed results with one result per parent step.
    """
    grouped, order = _group_results_by_step(step_results)
    collapsed: list[tuple[str, CqResult]] = []
    for step_id in order:
        group = grouped[step_id]
        if len(group) == 1:
            collapsed.append((step_id, _normalize_single_group_result(group[0])))
            continue

        collapsed.append((step_id, _merge_collapsed_q_group(group, ctx=ctx)))
    return collapsed


def _result_language(result: CqResult) -> QueryLanguage | None:
    value = result.summary.lang
    if value in {"python", "rust"}:
        return cast("QueryLanguage", value)
    return None


def _result_query_mode(result: CqResult) -> str | None:
    mode = result.summary.mode
    if isinstance(mode, str) and mode:
        return mode
    plan_summary = result.summary.plan
    if isinstance(plan_summary, dict):
        is_pattern_query = plan_summary.get("is_pattern_query")
        if isinstance(is_pattern_query, bool):
            return "pattern" if is_pattern_query else "entity"
    return None


def _result_match_count(result: CqResult | None) -> int:
    if result is None:
        return 0
    total = result.summary.total_matches
    if isinstance(total, int):
        return total
    matches = result.summary.matches
    if isinstance(matches, int):
        return matches
    return len(result.key_findings)


def _result_lang_scope(result: CqResult) -> QueryLanguageScope | None:
    value = result.summary.lang_scope
    if value in {"auto", "python", "rust"}:
        return cast("QueryLanguageScope", value)
    return None


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


def _group_results_by_step(
    step_results: list[tuple[str, CqResult]],
) -> tuple[dict[str, list[CqResult]], list[str]]:
    grouped: dict[str, list[CqResult]] = {}
    order: list[str] = []
    for step_id, result in step_results:
        if step_id not in grouped:
            grouped[step_id] = []
            order.append(step_id)
        grouped[step_id].append(result)
    return grouped, order


def _normalize_single_group_result(result: CqResult) -> CqResult:
    query_text = result.summary.query_text
    if isinstance(query_text, str) and query_text and result.summary.query is None:
        result.summary.query = query_text
    mode = _result_query_mode(result)
    if mode is not None and result.summary.mode is None:
        result.summary.mode = mode
    lang_scope = _result_lang_scope(result)
    if lang_scope is None:
        lang = _result_language(result)
        if lang is not None:
            lang_scope = lang
    if lang_scope is not None:
        if result.summary.lang_scope is None:
            result.summary.lang_scope = lang_scope
        if not result.summary.language_order:
            result.summary.language_order = list(expand_language_scope(lang_scope))
    result.summary.lang = None
    result.summary.query_text = None
    return result


def _collect_collapse_group_metadata(
    group: list[CqResult],
) -> tuple[dict[QueryLanguage, CqResult], str, str | None, QueryLanguageScope | None]:
    lang_results: dict[QueryLanguage, CqResult] = {}
    query_text = ""
    mode: str | None = None
    lang_scope: QueryLanguageScope | None = None
    for result in group:
        lang = _result_language(result)
        if lang is not None and lang not in lang_results:
            lang_results[lang] = result
        if not query_text:
            candidate = result.summary.query_text
            if isinstance(candidate, str):
                query_text = candidate
        if mode is None:
            mode = _result_query_mode(result)
        if lang_scope is None:
            lang_scope = _result_lang_scope(result)
    if lang_scope is None:
        if len(lang_results) == 1:
            lang_scope = next(iter(lang_results))
        elif len(lang_results) > 1:
            lang_scope = DEFAULT_QUERY_LANGUAGE_SCOPE
    return lang_results, query_text, mode, lang_scope


def _summary_common_for_collapse(
    query_text: str,
    mode: str | None,
    lang_scope: QueryLanguageScope | None,
) -> dict[str, object]:
    summary_common: dict[str, object] = {}
    if query_text:
        summary_common["query"] = query_text
    if mode is not None:
        summary_common["mode"] = mode
    if lang_scope is not None:
        summary_common["lang_scope"] = lang_scope
    return summary_common


def _merge_collapsed_q_group(group: list[CqResult], *, ctx: RunExecutionContext) -> CqResult:
    lang_results, query_text, mode, lang_scope = _collect_collapse_group_metadata(group)
    diagnostics = _build_collapse_diagnostics(lang_results, query_text)
    run = runmeta_for_scope_merge(
        macro="q",
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
    )
    return merge_language_cq_results(
        MergeResultsRequest(
            scope=DEFAULT_QUERY_LANGUAGE_SCOPE,
            results=lang_results,
            run=run,
            diagnostics=diagnostics,
            diagnostic_payloads=diagnostics_to_summary_payload(diagnostics),
            language_capabilities=build_language_capabilities(
                lang_scope=DEFAULT_QUERY_LANGUAGE_SCOPE
            ),
            summary_common=_summary_common_for_collapse(query_text, mode, lang_scope),
        )
    )


__all__ = [
    "collapse_parent_q_results",
]
