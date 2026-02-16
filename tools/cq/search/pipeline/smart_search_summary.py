"""Summary-building helpers for Smart Search results."""

from __future__ import annotations

from tools.cq.core.contracts import SummaryBuildRequest
from tools.cq.core.schema import DetailPayload, Finding
from tools.cq.orchestration.multilang_summary import (
    assert_multilang_summary,
    build_multilang_summary,
)
from tools.cq.query.language import (
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
    file_globs_for_scope,
    is_python_language,
    is_rust_language,
)
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search_telemetry import (
    build_enrichment_telemetry,
)
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    SearchStats,
    SearchSummaryInputs,
)
from tools.cq.search.semantic.diagnostics import (
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    is_python_oriented_query_text,
)
from tools.cq.search.tree_sitter.query.lint import lint_search_query_packs


def build_language_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    """Build summary payload for a search result.

    Returns:
    -------
    dict[str, object]
        Canonical multi-language summary payload.
    """
    config = inputs.config
    language_stats: dict[QueryLanguage, dict[str, object]] = {
        lang: {
            "scanned_files": stat.scanned_files,
            "scanned_files_is_estimate": stat.scanned_files_is_estimate,
            "matched_files": stat.matched_files,
            "total_matches": stat.total_matches,
            "timed_out": stat.timed_out,
            "truncated": stat.truncated,
            "caps_hit": (
                "timeout"
                if stat.timed_out
                else (
                    "max_files"
                    if stat.max_files_hit
                    else ("max_total_matches" if stat.max_matches_hit else "none")
                )
            ),
        }
        for lang, stat in inputs.language_stats.items()
    }
    common: dict[str, object] = {
        "query": config.query,
        "mode": config.mode.value,
        "mode_requested": config.mode_requested.value if config.mode_requested else "auto",
        "mode_effective": config.mode.value,
        "mode_chain": [mode.value for mode in (config.mode_chain or (config.mode,))],
        "fallback_applied": config.fallback_applied,
        "file_globs": inputs.file_globs or file_globs_for_scope(config.lang_scope),
        "include": config.include_globs or [],
        "exclude": config.exclude_globs or [],
        "context_lines": {
            "before": config.limits.context_before,
            "after": config.limits.context_after,
        },
        "limit": inputs.limit if inputs.limit is not None else config.limits.max_total_matches,
        "scanned_files": inputs.stats.scanned_files,
        "scanned_files_is_estimate": inputs.stats.scanned_files_is_estimate,
        "matched_files": inputs.stats.matched_files,
        "total_matches": inputs.stats.total_matches,
        "returned_matches": len(inputs.matches),
        "scan_method": "hybrid",
        "pattern": inputs.pattern,
        "case_sensitive": True,
        "with_neighborhood": bool(config.with_neighborhood),
        "caps_hit": (
            "timeout"
            if inputs.stats.timed_out
            else (
                "max_files"
                if inputs.stats.max_files_hit
                else ("max_total_matches" if inputs.stats.max_matches_hit else "none")
            )
        ),
        "truncated": inputs.stats.truncated,
        "timed_out": inputs.stats.timed_out,
        "python_semantic_overview": dict[str, object](),
        "python_semantic_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "rust_semantic_telemetry": {
            "attempted": 0,
            "applied": 0,
            "failed": 0,
            "skipped": 0,
            "timed_out": 0,
        },
        "semantic_planes": dict[str, object](),
        "python_semantic_diagnostics": list[dict[str, object]](),
    }
    if config.tc is not None:
        common["rg_pcre2_available"] = bool(getattr(config.tc, "rg_pcre2_available", False))
        common["rg_pcre2_version"] = getattr(config.tc, "rg_pcre2_version", None)
    if isinstance(inputs.stats.rg_stats, dict):
        common["rg_stats"] = inputs.stats.rg_stats.copy()
    return build_multilang_summary(
        SummaryBuildRequest(
            common=common,
            lang_scope=config.lang_scope,
            language_order=inputs.languages,
            languages=language_stats,
            cross_language_diagnostics=[],
            language_capabilities=build_language_capabilities(lang_scope=config.lang_scope),
        )
    )


def _build_cross_language_diagnostics_for_search(
    *,
    query: str,
    lang_scope: QueryLanguageScope,
    python_matches: int,
    rust_matches: int,
) -> list[Finding]:
    return build_cross_language_diagnostics(
        lang_scope=lang_scope,
        python_matches=python_matches,
        rust_matches=rust_matches,
        python_oriented=is_python_oriented_query_text(query),
    )


def _build_capability_diagnostics_for_search(*, lang_scope: QueryLanguageScope) -> list[Finding]:
    from tools.cq.search.semantic.diagnostics import build_capability_diagnostics

    return build_capability_diagnostics(
        features=["pattern_query"],
        lang_scope=lang_scope,
    )


def _build_tree_sitter_runtime_diagnostics(
    telemetry: dict[str, object],
) -> list[Finding]:
    python_bucket = telemetry.get("python")
    if not isinstance(python_bucket, dict):
        return []
    runtime = python_bucket.get("query_runtime")
    if not isinstance(runtime, dict):
        return []
    did_exceed = int(runtime.get("did_exceed_match_limit", 0) or 0)
    cancelled = int(runtime.get("cancelled", 0) or 0)
    findings: list[Finding] = []
    if did_exceed > 0:
        findings.append(
            Finding(
                category="tree_sitter_runtime",
                message=f"tree-sitter match limit exceeded on {did_exceed} anchors",
                severity="warning",
                details=DetailPayload(
                    kind="tree_sitter_runtime",
                    data={
                        "reason": "did_exceed_match_limit",
                        "count": did_exceed,
                    },
                ),
            )
        )
    if cancelled > 0:
        findings.append(
            Finding(
                category="tree_sitter_runtime",
                message=f"tree-sitter query cancelled on {cancelled} anchors",
                severity="warning",
                details=DetailPayload(
                    kind="tree_sitter_runtime",
                    data={
                        "reason": "cancelled",
                        "count": cancelled,
                    },
                ),
            )
        )
    return findings


def build_search_summary(
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
    enriched_matches: list[EnrichedMatch],
    *,
    python_semantic_overview: dict[str, object],
    python_semantic_telemetry: dict[str, object],
    python_semantic_diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], list[Finding]]:
    """Build full search summary and diagnostics payload.

    Returns:
    -------
    tuple[dict[str, object], list[Finding]]
        Summary payload and synthesized diagnostics.
    """
    language_stats: dict[QueryLanguage, SearchStats] = {
        result.lang: result.stats for result in partition_results
    }
    patterns = {result.lang: result.pattern for result in partition_results}
    merged_stats = SearchStats(
        scanned_files=sum(stat.scanned_files for stat in language_stats.values()),
        matched_files=sum(stat.matched_files for stat in language_stats.values()),
        total_matches=sum(stat.total_matches for stat in language_stats.values()),
        scanned_files_is_estimate=any(
            stat.scanned_files_is_estimate for stat in language_stats.values()
        ),
        truncated=any(stat.truncated for stat in language_stats.values()),
        timed_out=any(stat.timed_out for stat in language_stats.values()),
        max_files_hit=any(stat.max_files_hit for stat in language_stats.values()),
        max_matches_hit=any(stat.max_matches_hit for stat in language_stats.values()),
    )
    summary_inputs = SearchSummaryInputs(
        config=ctx,
        stats=merged_stats,
        matches=enriched_matches,
        languages=tuple(expand_language_scope(ctx.lang_scope)),
        language_stats=language_stats,
        file_globs=file_globs_for_scope(ctx.lang_scope),
        limit=ctx.limits.max_total_matches,
        pattern=patterns.get("python") or next(iter(patterns.values()), None),
    )
    summary = build_language_summary(summary_inputs)
    dropped_by_scope = {
        result.lang: result.dropped_by_scope
        for result in partition_results
        if result.dropped_by_scope > 0
    }
    python_matches = sum(1 for match in enriched_matches if is_python_language(match.language))
    rust_matches = sum(1 for match in enriched_matches if is_rust_language(match.language))
    diagnostics = _build_cross_language_diagnostics_for_search(
        query=ctx.query,
        lang_scope=ctx.lang_scope,
        python_matches=python_matches,
        rust_matches=rust_matches,
    )
    capability_diagnostics = _build_capability_diagnostics_for_search(lang_scope=ctx.lang_scope)
    all_diagnostics = diagnostics + capability_diagnostics
    query_pack_lint = lint_search_query_packs()
    summary["query_pack_lint"] = {
        "status": query_pack_lint.status,
        "errors": list(query_pack_lint.errors),
    }
    if query_pack_lint.status != "ok":
        all_diagnostics.append(
            Finding(
                category="query_pack_lint",
                message=f"Query pack lint failed: {len(query_pack_lint.errors)} errors",
                severity="warning",
                details=DetailPayload(
                    kind="query_pack_lint",
                    data={"errors": list(query_pack_lint.errors)},
                ),
            )
        )
    enrichment_telemetry = build_enrichment_telemetry(enriched_matches)
    all_diagnostics.extend(_build_tree_sitter_runtime_diagnostics(enrichment_telemetry))
    summary["cross_language_diagnostics"] = diagnostics_to_summary_payload(all_diagnostics)
    summary["language_capabilities"] = build_language_capabilities(lang_scope=ctx.lang_scope)
    summary["enrichment_telemetry"] = enrichment_telemetry
    summary["python_semantic_overview"] = python_semantic_overview
    summary["python_semantic_telemetry"] = python_semantic_telemetry
    summary.setdefault(
        "rust_semantic_telemetry",
        {"attempted": 0, "applied": 0, "failed": 0, "skipped": 0, "timed_out": 0},
    )
    summary["python_semantic_diagnostics"] = python_semantic_diagnostics
    if dropped_by_scope:
        summary["dropped_by_scope"] = dropped_by_scope
    assert_multilang_summary(summary)
    return summary, all_diagnostics


__all__ = ["build_language_summary", "build_search_summary"]
