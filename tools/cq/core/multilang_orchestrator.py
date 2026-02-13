"""Shared multi-language orchestration and merge helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

from tools.cq.core.multilang_summary import (
    build_multilang_summary,
    partition_stats_from_result_summary,
)
from tools.cq.core.requests import MergeResultsRequest, SummaryBuildRequest
from tools.cq.core.run_context import RunContext
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import CqResult, DetailPayload, Finding, Section, mk_result
from tools.cq.query.language import (
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
)

if TYPE_CHECKING:
    from tools.cq.core.front_door_insight import FrontDoorInsightV1
    from tools.cq.core.schema import RunMeta
    from tools.cq.core.toolchain import Toolchain

T = TypeVar("T")


def language_priority(scope: QueryLanguageScope) -> dict[QueryLanguage, int]:
    """Return deterministic language ordering for a scope.

    Returns:
    -------
    dict[QueryLanguage, int]
        Language rank mapping used by merge ordering.
    """
    return {lang: idx for idx, lang in enumerate(expand_language_scope(scope))}


def execute_by_language_scope[T](
    scope: QueryLanguageScope,
    run_one: Callable[[QueryLanguage], T],
) -> dict[QueryLanguage, T]:
    """Execute a callback once per language in scope.

    Returns:
    -------
    dict[QueryLanguage, T]
        Per-language execution outputs.
    """
    languages = tuple(expand_language_scope(scope))
    if len(languages) == 0:
        return {}
    if len(languages) == 1:
        only_language = languages[0]
        return {only_language: run_one(only_language)}

    scheduler = get_worker_scheduler()
    policy = scheduler.policy
    if policy.query_partition_workers <= 1:
        return {lang: run_one(lang) for lang in languages}

    futures = [scheduler.submit_io(run_one, lang) for lang in languages]
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=max(1.0, float(len(languages)) * 5.0),
    )
    if batch.timed_out > 0:
        return {lang: run_one(lang) for lang in languages}
    return dict(zip(languages, batch.done, strict=False))


def merge_partitioned_items(
    *,
    partitions: Mapping[QueryLanguage, list[T]],
    scope: QueryLanguageScope,
    get_language: Callable[[T], QueryLanguage],
    get_score: Callable[[T], float],
    get_location: Callable[[T], tuple[str, int, int]],
) -> list[T]:
    """Merge and sort language partitions with stable deterministic ordering.

    Returns:
    -------
    list[T]
        Flattened and ordered items across all language partitions.
    """
    priority = language_priority(scope)
    merged: list[T] = []
    for lang in expand_language_scope(scope):
        merged.extend(partitions.get(lang, []))
    for lang, items in partitions.items():
        if lang not in priority:
            merged.extend(items)
    merged.sort(
        key=lambda item: (
            priority.get(get_language(item), 99),
            -get_score(item),
            *get_location(item),
        )
    )
    return merged


def _finding_score(finding: Finding) -> float:
    score = finding.details.score
    if score is None:
        return 0.0
    impact = score.impact_score or 0.0
    confidence = score.confidence_score or 0.0
    return impact + confidence


def _finding_location(finding: Finding) -> tuple[str, int, int]:
    anchor = finding.anchor
    if anchor is None:
        return ("", 0, 0)
    col = anchor.col if anchor.col is not None else 0
    return (anchor.file, anchor.line, col)


def _clone_finding_with_language(
    finding: Finding,
    *,
    lang: QueryLanguage,
) -> Finding:
    data = dict(finding.details.data)
    data.setdefault("language", lang)
    details = DetailPayload(
        kind=finding.details.kind or finding.category,
        score=finding.details.score,
        data=data,
    )
    return Finding(
        category=finding.category,
        message=finding.message,
        anchor=finding.anchor,
        severity=finding.severity,
        details=details,
    )


def _result_match_count(result: CqResult) -> int:
    summary_matches = result.summary.get("matches")
    if isinstance(summary_matches, int):
        return summary_matches
    summary_total = result.summary.get("total_matches")
    if isinstance(summary_total, int):
        return summary_total
    return len(result.key_findings)


def _select_front_door_insight(
    scope: QueryLanguageScope,
    results: Mapping[QueryLanguage, CqResult],
) -> FrontDoorInsightV1 | None:
    from tools.cq.core.front_door_insight import (
        coerce_front_door_insight,
        mark_partial_for_missing_languages,
    )

    order = list(expand_language_scope(scope))
    by_language = _collect_insights_by_language(
        order=order,
        results=results,
        coerce_front_door_insight=coerce_front_door_insight,
    )
    if not by_language:
        return None

    selected = _select_ordered_insight(
        order=order,
        by_language=by_language,
        require_grounded=True,
    )
    if selected is None:
        selected = _select_ordered_insight(
            order=order,
            by_language=by_language,
            require_grounded=False,
        )
    if selected is None:
        return None

    missing_languages = [lang for lang in order if lang not in by_language]
    if missing_languages:
        selected = mark_partial_for_missing_languages(
            selected,
            missing_languages=missing_languages,
        )
    return selected


def _collect_insights_by_language(
    *,
    order: list[QueryLanguage],
    results: Mapping[QueryLanguage, CqResult],
    coerce_front_door_insight: Callable[[object], FrontDoorInsightV1 | None],
) -> dict[QueryLanguage, FrontDoorInsightV1]:
    by_language: dict[QueryLanguage, FrontDoorInsightV1] = {}
    for lang in order:
        result = results.get(lang)
        if result is None:
            continue
        insight = coerce_front_door_insight(result.summary.get("front_door_insight"))
        if insight is not None:
            by_language[lang] = insight
    return by_language


def _is_grounded_insight(insight: FrontDoorInsightV1) -> bool:
    location = insight.target.location
    if location.file:
        return True
    return insight.target.kind not in {"query", "unknown", "entity"}


def _select_ordered_insight(
    *,
    order: list[QueryLanguage],
    by_language: Mapping[QueryLanguage, FrontDoorInsightV1],
    require_grounded: bool,
) -> FrontDoorInsightV1 | None:
    for lang in order:
        candidate = by_language.get(lang)
        if candidate is None:
            continue
        if not require_grounded or _is_grounded_insight(candidate):
            return candidate
    return None


def merge_language_cq_results(request: MergeResultsRequest) -> CqResult:
    """Merge per-language CQ results into a canonical multi-language result.

    Returns:
    -------
    CqResult
        Unified result with deterministic ordering and multilang summary.
    """
    merged = mk_result(request.run)
    order = list(expand_language_scope(request.scope))
    priority = language_priority(request.scope)
    partitions: dict[QueryLanguage, dict[str, object]] = {}

    for lang in order:
        result = request.results.get(lang)
        if result is None:
            partitions[lang] = {}
            continue
        partitions[lang] = partition_stats_from_result_summary(
            result.summary,
            fallback_matches=_result_match_count(result),
        )
        merged.key_findings.extend(
            _clone_finding_with_language(finding, lang=lang) for finding in result.key_findings
        )
        merged.evidence.extend(
            _clone_finding_with_language(finding, lang=lang) for finding in result.evidence
        )
        for section in result.sections:
            title = (
                f"{lang}: {section.title}"
                if request.include_section_language_prefix
                else section.title
            )
            merged.sections.append(
                Section(
                    title=title,
                    findings=[
                        _clone_finding_with_language(finding, lang=lang)
                        for finding in section.findings
                    ],
                    collapsed=section.collapsed,
                )
            )
        merged.artifacts.extend(result.artifacts)

    for lang, result in request.results.items():
        if lang in partitions:
            continue
        partitions[lang] = partition_stats_from_result_summary(
            result.summary,
            fallback_matches=_result_match_count(result),
        )

    merged.key_findings.sort(
        key=lambda finding: (
            priority.get(cast("QueryLanguage", finding.details.data.get("language", "python")), 99),
            -_finding_score(finding),
            *_finding_location(finding),
            finding.message,
        )
    )
    merged.evidence.sort(
        key=lambda finding: (
            priority.get(cast("QueryLanguage", finding.details.data.get("language", "python")), 99),
            -_finding_score(finding),
            *_finding_location(finding),
            finding.message,
        )
    )

    diag_findings: list[Finding] = (
        list(request.diagnostics) if request.diagnostics is not None else []
    )
    if diag_findings:
        merged.sections.append(Section(title="Cross-Language Diagnostics", findings=diag_findings))
    summary_diagnostics = request.diagnostic_payloads
    if summary_diagnostics is None:
        summary_diagnostics = [
            {
                "code": "ML000",
                "severity": finding.severity,
                "message": finding.message,
                "intent": "unspecified",
                "languages": cast("list[str]", []),
                "counts": cast("dict[str, int]", {}),
                "remediation": "",
            }
            for finding in diag_findings
        ]
    summary_common = dict(request.summary_common or {})
    summary_common.setdefault("pyrefly_overview", {})
    summary_common.setdefault(
        "pyrefly_telemetry",
        {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
    )
    summary_common.setdefault(
        "rust_lsp_telemetry",
        {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
    )
    summary_common.setdefault("pyrefly_diagnostics", [])
    summary_common.setdefault("lsp_advanced_planes", {})

    merged.summary = build_multilang_summary(
        SummaryBuildRequest(
            common=summary_common,
            lang_scope=request.scope,
            language_order=tuple(order),
            languages=partitions,
            cross_language_diagnostics=summary_diagnostics,
            language_capabilities=request.language_capabilities,
        )
    )
    merged_insight = _select_front_door_insight(request.scope, request.results)
    if merged_insight is not None:
        from tools.cq.core.front_door_insight import to_public_front_door_insight_dict

        merged.summary["front_door_insight"] = to_public_front_door_insight_dict(merged_insight)
    if diag_findings:
        merged.key_findings.extend(diag_findings)
    return merged


def runmeta_for_scope_merge(
    *,
    macro: str,
    root: Path,
    argv: list[str],
    tc: Toolchain | None,
) -> RunMeta:
    """Create run metadata for merged multi-language results.

    Returns:
    -------
    RunMeta
        Run metadata for a merged multi-language CQ response.
    """
    from tools.cq.core.schema import ms

    run_ctx = RunContext.from_parts(root=root, argv=argv, tc=tc, started_ms=ms())
    return run_ctx.to_runmeta(macro)


__all__ = [
    "execute_by_language_scope",
    "language_priority",
    "merge_language_cq_results",
    "merge_partitioned_items",
    "runmeta_for_scope_merge",
]
