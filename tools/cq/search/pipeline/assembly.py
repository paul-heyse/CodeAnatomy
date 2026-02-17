"""Smart Search result assembly from enriched matches."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.maintenance import maintenance_tick
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import Anchor, CqResult, Finding, Section, assign_result_finding_ids
from tools.cq.core.summary_contract import SearchSummaryV1, as_search_summary, summary_from_mapping
from tools.cq.search.objects.resolve import ObjectResolutionRuntime
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.python_semantic import merge_matches_and_python_semantic
from tools.cq.search.pipeline.search_object_view_store import register_search_object_view
from tools.cq.search.pipeline.search_semantic import apply_search_semantic_insight
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    _NeighborhoodPreviewInputs,
    _SearchAssemblyInputs,
)

if TYPE_CHECKING:
    from tools.cq.core.front_door_contracts import (
        FrontDoorInsightV1,
        InsightNeighborhoodV1,
        InsightRiskV1,
    )
    from tools.cq.search.objects.render import SearchObjectSummaryV1

# Evidence disclosure cap to keep output high-signal
MAX_EVIDENCE = 100
MAX_TARGET_CANDIDATES = 3
_TARGET_CANDIDATE_KINDS: frozenset[str] = frozenset(
    {
        "function",
        "method",
        "class",
        "type",
        "module",
        "callable",
    }
)


def _normalize_neighborhood_file_path(path: str) -> str:
    """Normalize neighborhood file path to canonical form.

    Parameters
    ----------
    path
        File path string to normalize.

    Returns:
    -------
    str
        Normalized POSIX path.
    """
    normalized = path.strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return Path(normalized).as_posix()


def _normalize_target_candidate_kind(kind: str | None) -> str:
    """Normalize target candidate kind to canonical form.

    Parameters
    ----------
    kind
        Kind string from object reference.

    Returns:
    -------
    str
        Normalized kind identifier.
    """
    if not isinstance(kind, str) or not kind:
        return "reference"
    lowered = kind.lower()
    if lowered in {"method", "function", "callable"}:
        return "function"
    if lowered in {"class"}:
        return "class"
    if lowered in {"module"}:
        return "module"
    if lowered in {"type"}:
        return "type"
    return lowered


def _build_object_candidate_finding(
    *,
    summary: SearchObjectSummaryV1,
    representative: EnrichedMatch | None,
) -> Finding:
    """Build finding from object summary for target candidates section.

    Parameters
    ----------
    summary
        Object summary from resolution runtime.
    representative
        Representative match for this object (if available).

    Returns:
    -------
    Finding
        Target candidate finding.
    """
    from tools.cq.core.schema import DetailPayload

    object_ref = summary.object_ref
    kind = _normalize_target_candidate_kind(object_ref.kind)
    symbol = object_ref.symbol
    anchor: Anchor | None = None
    if isinstance(object_ref.canonical_file, str) and isinstance(object_ref.canonical_line, int):
        anchor = Anchor(
            file=object_ref.canonical_file,
            line=max(1, int(object_ref.canonical_line)),
            col=representative.col if representative is not None else None,
        )
    elif representative is not None:
        anchor = Anchor.from_span(representative.span)
    details = {
        "name": symbol,
        "kind": kind,
        "qualified_name": object_ref.qualified_name,
        "signature": representative.text.strip() if representative is not None else symbol,
        "object_id": object_ref.object_id,
        "language": representative.language if representative is not None else object_ref.language,
        "occurrence_count": summary.occurrence_count,
        "resolution_quality": object_ref.resolution_quality,
    }
    return Finding(
        category="definition",
        message=f"{kind}: {symbol}",
        anchor=anchor,
        severity="info",
        details=DetailPayload(kind=kind, data=cast("dict[str, object]", details)),
    )


def _collect_definition_candidates(
    ctx: SearchConfig,
    object_runtime: ObjectResolutionRuntime,
) -> tuple[list[EnrichedMatch], list[Finding]]:
    """Collect definition candidates for target candidates section.

    Parameters
    ----------
    ctx
        Search context with configuration.
    object_runtime
        Object resolution runtime with resolved view.

    Returns:
    -------
    tuple[list[EnrichedMatch], list[Finding]]
        Definition matches and candidate findings.
    """
    _ = ctx
    definition_matches: list[EnrichedMatch] = []
    candidate_findings: list[Finding] = []

    for summary in object_runtime.view.summaries:
        object_ref = summary.object_ref
        kind = _normalize_target_candidate_kind(object_ref.kind)
        if kind not in _TARGET_CANDIDATE_KINDS:
            continue

        representative = object_runtime.representative_matches.get(object_ref.object_id)
        if representative is not None:
            definition_matches.append(representative)
        candidate = _build_object_candidate_finding(
            summary=summary,
            representative=representative,
        )
        candidate_findings.append(candidate)
        if len(candidate_findings) >= MAX_TARGET_CANDIDATES:
            break

    if candidate_findings:
        return definition_matches, candidate_findings

    # Fallback: always provide at least one target candidate from top object.
    if not object_runtime.view.summaries:
        return definition_matches, candidate_findings
    fallback_summary = object_runtime.view.summaries[0]
    representative = object_runtime.representative_matches.get(
        fallback_summary.object_ref.object_id
    )
    if representative is not None:
        definition_matches.append(representative)
    candidate_findings.append(
        _build_object_candidate_finding(summary=fallback_summary, representative=representative)
    )
    return definition_matches, candidate_findings


def _resolve_primary_target_match(
    *,
    candidate_findings: list[Finding],
    object_runtime: ObjectResolutionRuntime,
    definition_matches: list[EnrichedMatch],
    enriched_matches: list[EnrichedMatch],
) -> EnrichedMatch | None:
    """Resolve primary target match from candidates.

    Parameters
    ----------
    candidate_findings
        Target candidate findings.
    object_runtime
        Object resolution runtime.
    definition_matches
        Definition matches.
    enriched_matches
        All enriched matches.

    Returns:
    -------
    EnrichedMatch | None
        Primary target match if resolved.
    """
    if candidate_findings:
        object_id = candidate_findings[0].details.get("object_id")
        if isinstance(object_id, str):
            representative = object_runtime.representative_matches.get(object_id)
            if representative is not None:
                return representative
    if definition_matches:
        return definition_matches[0]
    if object_runtime.view.summaries:
        object_id = object_runtime.view.summaries[0].object_ref.object_id
        representative = object_runtime.representative_matches.get(object_id)
        if representative is not None:
            return representative
    if enriched_matches:
        return enriched_matches[0]
    return None


def _candidate_scope_paths_for_neighborhood(
    *,
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
) -> tuple[Path, ...]:
    """Collect candidate scope paths from partition results.

    Parameters
    ----------
    ctx
        Search context with configuration.
    partition_results
        Language partition results.

    Returns:
    -------
    tuple[Path, ...]
        Ordered scope paths from matched files.
    """
    ordered_paths: dict[str, Path] = {}
    for partition in partition_results:
        for match in partition.raw_matches:
            rel_path = str(match.file).strip()
            if not rel_path or rel_path in ordered_paths:
                continue
            candidate = (ctx.root / rel_path).resolve()
            if candidate.exists():
                ordered_paths[rel_path] = candidate
    return tuple(ordered_paths.values())


def _build_structural_neighborhood_preview(
    ctx: SearchConfig,
    *,
    primary_target_finding: Finding | None,
    definition_matches: list[EnrichedMatch],
) -> tuple[InsightNeighborhoodV1 | None, list[Finding], list[str]]:
    """Build structural neighborhood preview from tree-sitter collector.

    Parameters
    ----------
    ctx
        Search context with configuration.
    primary_target_finding
        Primary target finding for anchor resolution.
    definition_matches
        Definition matches for language inference.

    Returns:
    -------
    tuple[InsightNeighborhoodV1 | None, list[Finding], list[str]]
        Neighborhood, findings, and degradation notes.
    """
    from tools.cq.core.front_door_assembly import build_neighborhood_from_slices
    from tools.cq.core.schema import DetailPayload
    from tools.cq.neighborhood.contracts import TreeSitterNeighborhoodCollectRequest
    from tools.cq.neighborhood.tree_sitter_collector import collect_tree_sitter_neighborhood

    if primary_target_finding is None or primary_target_finding.anchor is None:
        return None, [], []

    target_name = (
        str(primary_target_finding.details.get("name", "")).strip()
        or primary_target_finding.message.split(":")[-1].strip()
        or ctx.query
    )
    target_file = _normalize_neighborhood_file_path(primary_target_finding.anchor.file)
    target_language = (
        definition_matches[0].language
        if definition_matches
        else str(primary_target_finding.details.get("language", "python"))
    )
    collector_language = "rust" if target_language == "rust" else "python"
    try:
        collect_result = collect_tree_sitter_neighborhood(
            TreeSitterNeighborhoodCollectRequest(
                root=str(ctx.root),
                target_name=target_name,
                target_file=target_file,
                language=collector_language,
                target_line=primary_target_finding.anchor.line,
                target_col=int(primary_target_finding.anchor.col or 0),
                max_per_slice=5,
            )
        )
    except (OSError, RuntimeError, TimeoutError, ValueError, TypeError) as exc:
        return None, [], [f"tree_sitter_neighborhood_unavailable:{type(exc).__name__}"]

    slices = tuple(collect_result.slices)
    degrades = tuple(collect_result.diagnostics)

    neighborhood = build_neighborhood_from_slices(
        slices,
        preview_per_slice=5,
        source="structural",
    )
    findings: list[Finding] = []
    for slice_item in slices:
        labels = [
            node.display_label or node.name
            for node in slice_item.preview[:5]
            if (node.display_label or node.name)
        ]
        message = f"{slice_item.title}: {slice_item.total}"
        if labels:
            message += f" (top: {', '.join(labels)})"
        findings.append(
            Finding(
                category="neighborhood",
                message=message,
                severity="info",
                details=DetailPayload(
                    kind="neighborhood",
                    data={
                        "slice_kind": slice_item.kind,
                        "total": slice_item.total,
                        "preview": labels,
                    },
                ),
            )
        )
    notes = [f"{degrade.stage}:{degrade.category or degrade.severity}" for degrade in degrades]
    return neighborhood, findings, list(dict.fromkeys(notes))


def _build_tree_sitter_neighborhood_preview(
    *,
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
    summary: SearchSummaryV1,
    sections: list[Section],
    inputs: _NeighborhoodPreviewInputs,
) -> tuple[InsightNeighborhoodV1 | None, list[str]]:
    """Build tree-sitter neighborhood preview and insert into sections.

    Parameters
    ----------
    ctx
        Search context with configuration.
    partition_results
        Language partition results.
    summary
        Summary payload to update.
    sections
        Sections list to insert neighborhood findings into.
    inputs
        Neighborhood preview input parameters.

    Returns:
    -------
    tuple[InsightNeighborhoodV1 | None, list[str]]
        Insight neighborhood and degradation notes.
    """
    from tools.cq.search.pipeline.orchestration import insert_neighborhood_preview

    summary.tree_sitter_neighborhood = {
        "enabled": bool(ctx.with_neighborhood),
        "mode": "opt_in",
    }
    if not ctx.with_neighborhood:
        return None, ["tree_sitter_neighborhood_disabled_by_default"]

    scope_paths = _candidate_scope_paths_for_neighborhood(
        ctx=ctx,
        partition_results=partition_results,
    )
    neighborhood_summary = dict(summary.tree_sitter_neighborhood)
    neighborhood_summary["candidate_scope_files"] = str(len(scope_paths))
    summary.tree_sitter_neighborhood = neighborhood_summary
    insight_neighborhood, neighborhood_findings, neighborhood_notes = (
        _build_structural_neighborhood_preview(
            ctx,
            primary_target_finding=inputs.primary_target_finding,
            definition_matches=inputs.definition_matches,
        )
    )
    insert_neighborhood_preview(
        sections,
        findings=neighborhood_findings,
        has_target_candidates=inputs.has_target_candidates,
    )
    return insight_neighborhood, neighborhood_notes


def _build_search_result_key_findings(inputs: _SearchAssemblyInputs) -> list[Finding]:
    """Build key findings list from assembly inputs.

    Parameters
    ----------
    inputs
        Search assembly inputs.

    Returns:
    -------
    list[Finding]
        Key findings for CqResult.
    """
    key_findings = list(
        inputs.candidate_findings or (inputs.sections[0].findings[:5] if inputs.sections else [])
    )
    key_findings.extend(inputs.all_diagnostics)
    return key_findings


def _build_search_risk(
    neighborhood: InsightNeighborhoodV1 | None,
) -> InsightRiskV1 | None:
    """Build risk assessment from neighborhood counters.

    Parameters
    ----------
    neighborhood
        Insight neighborhood with caller/callee totals.

    Returns:
    -------
    InsightRiskV1 | None
        Risk assessment or None if no neighborhood available.
    """
    from tools.cq.core.front_door_contracts import InsightRiskCountersV1
    from tools.cq.core.front_door_risk import risk_from_counters

    if neighborhood is None:
        return None
    return risk_from_counters(
        InsightRiskCountersV1(
            callers=neighborhood.callers.total,
            callees=neighborhood.callees.total,
        )
    )


def _assemble_search_insight(
    ctx: SearchConfig,
    inputs: _SearchAssemblyInputs,
) -> FrontDoorInsightV1:
    """Assemble front-door insight from search assembly inputs.

    Parameters
    ----------
    ctx
        Search context with configuration.
    inputs
        Search assembly inputs.

    Returns:
    -------
    FrontDoorInsightV1
        Assembled insight card with semantic enrichment.
    """
    from tools.cq.core.front_door_assembly import build_search_insight
    from tools.cq.core.front_door_contracts import SearchInsightBuildRequestV1

    insight = build_search_insight(
        SearchInsightBuildRequestV1(
            summary=inputs.summary,
            primary_target=inputs.primary_target_finding,
            target_candidates=tuple(inputs.candidate_findings),
            neighborhood=inputs.insight_neighborhood,
            risk=_build_search_risk(inputs.insight_neighborhood),
        )
    )
    if inputs.neighborhood_notes:
        insight = msgspec.structs.replace(
            insight,
            degradation=msgspec.structs.replace(
                insight.degradation,
                notes=tuple(
                    dict.fromkeys([*insight.degradation.notes, *inputs.neighborhood_notes])
                ),
            ),
        )
    return apply_search_semantic_insight(
        ctx=ctx,
        insight=insight,
        summary=inputs.summary,
        primary_target_finding=inputs.primary_target_finding,
        primary_target_match=inputs.primary_target_match,
    )


def _prepare_search_assembly_inputs(
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
) -> _SearchAssemblyInputs:
    """Prepare all assembly inputs from partition results.

    Parameters
    ----------
    ctx
        Search context with configuration.
    partition_results
        Language partition results.

    Returns:
    -------
    _SearchAssemblyInputs
        Complete assembly inputs for result construction.
    """
    from tools.cq.search.objects.resolve import build_object_resolved_view
    from tools.cq.search.pipeline.smart_search_sections import build_sections
    from tools.cq.search.pipeline.smart_search_summary import build_search_summary

    (
        enriched_matches,
        python_semantic_overview,
        python_semantic_telemetry,
        python_semantic_diagnostics,
    ) = merge_matches_and_python_semantic(ctx, partition_results)
    summary_raw, all_diagnostics = build_search_summary(
        ctx,
        partition_results,
        enriched_matches,
        python_semantic_overview=python_semantic_overview,
        python_semantic_telemetry=python_semantic_telemetry,
        python_semantic_diagnostics=python_semantic_diagnostics,
    )
    summary = as_search_summary(summary_from_mapping(summary_raw))
    object_runtime = build_object_resolved_view(enriched_matches, query=ctx.query)
    summary.resolved_objects = len(object_runtime.view.summaries)
    summary.resolved_occurrences = len(object_runtime.view.occurrences)
    sections = build_sections(
        enriched_matches,
        ctx.root,
        ctx.query,
        ctx.mode,
        include_strings=ctx.include_strings,
        object_runtime=object_runtime,
    )
    if all_diagnostics:
        sections.append(Section(title="Cross-Language Diagnostics", findings=all_diagnostics))
    definition_matches, candidate_findings = _collect_definition_candidates(ctx, object_runtime)
    from tools.cq.search.pipeline.orchestration import insert_target_candidates

    insert_target_candidates(sections, candidates=candidate_findings)
    primary_target_finding = candidate_findings[0] if candidate_findings else None
    primary_target_match = _resolve_primary_target_match(
        candidate_findings=candidate_findings,
        object_runtime=object_runtime,
        definition_matches=definition_matches,
        enriched_matches=enriched_matches,
    )
    insight_neighborhood, neighborhood_notes = _build_tree_sitter_neighborhood_preview(
        ctx=ctx,
        partition_results=partition_results,
        summary=summary,
        sections=sections,
        inputs=_NeighborhoodPreviewInputs(
            primary_target_finding=primary_target_finding,
            definition_matches=definition_matches,
            has_target_candidates=bool(candidate_findings),
        ),
    )
    return _SearchAssemblyInputs(
        enriched_matches=enriched_matches,
        object_runtime=object_runtime,
        summary=summary,
        sections=sections,
        all_diagnostics=all_diagnostics,
        definition_matches=definition_matches,
        candidate_findings=candidate_findings,
        primary_target_finding=primary_target_finding,
        primary_target_match=primary_target_match,
        insight_neighborhood=insight_neighborhood,
        neighborhood_notes=neighborhood_notes,
    )


def _assemble_smart_search_result(
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
) -> CqResult:
    """Assemble final CqResult from search context and partition results.

    Parameters
    ----------
    ctx
        Search context with configuration.
    partition_results
        Language partition results.

    Returns:
    -------
    CqResult
        Complete assembled search result.
    """
    from tools.cq.core.front_door_render import to_public_front_door_insight_dict
    from tools.cq.search.pipeline.smart_search_sections import build_finding

    inputs = _prepare_search_assembly_inputs(ctx, partition_results)
    insight = _assemble_search_insight(ctx, inputs)
    inputs.summary.front_door_insight = to_public_front_door_insight_dict(insight)

    run_ctx = RunContext.from_parts(
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.tc,
        started_ms=ctx.started_ms,
        run_id=ctx.run_id,
    )
    run = run_ctx.to_runmeta("search")
    result = CqResult(
        run=run,
        summary=inputs.summary,
        sections=inputs.sections,
        key_findings=_build_search_result_key_findings(inputs),
        evidence=[build_finding(m, ctx.root) for m in inputs.enriched_matches[:MAX_EVIDENCE]],
    )
    register_search_object_view(run_id=run.run_id, view=inputs.object_runtime.view)
    result.summary.cache_backend = snapshot_backend_metrics(root=ctx.root)
    cache_maintenance_payload = contract_to_builtins(
        maintenance_tick(get_cq_cache_backend(root=ctx.root))
    )
    as_search_summary(result.summary).cache_maintenance = (
        cache_maintenance_payload if isinstance(cache_maintenance_payload, dict) else {}
    )
    return assign_result_finding_ids(result)


def assemble_smart_search_result(
    ctx: SearchConfig,
    partition_results: list[LanguageSearchResult],
) -> CqResult:
    """Assemble Smart Search result from context and partition results.

    Public wrapper for internal assembly logic.

    Parameters
    ----------
    ctx
        Search context with all configuration and state.
    partition_results
        Language-partitioned search results from discovery phase.

    Returns:
    -------
    CqResult
        Assembled search result with sections and insights.
    """
    return _assemble_smart_search_result(ctx, partition_results)


__all__ = [
    "assemble_smart_search_result",
]
