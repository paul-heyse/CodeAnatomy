"""Smart Search result assembly from enriched matches."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.core.cache.maintenance import maintenance_tick
from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, Finding, Section, assign_result_finding_ids
from tools.cq.core.summary_contract import as_search_summary, summary_from_mapping
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.neighborhood_preview import (
    build_tree_sitter_neighborhood_preview as _build_tree_sitter_neighborhood_preview,
)
from tools.cq.search.pipeline.search_object_view_store import register_search_object_view
from tools.cq.search.pipeline.smart_search_types import (
    LanguageSearchResult,
    _NeighborhoodPreviewInputs,
    _SearchAssemblyInputs,
)
from tools.cq.search.pipeline.target_resolution import (
    collect_definition_candidates as _collect_definition_candidates,
)
from tools.cq.search.pipeline.target_resolution import (
    resolve_primary_target_match as _resolve_primary_target_match,
)

if TYPE_CHECKING:
    from tools.cq.core.front_door_contracts import (
        FrontDoorInsightV1,
        InsightNeighborhoodV1,
        InsightRiskV1,
    )

# Evidence disclosure cap to keep output high-signal
MAX_EVIDENCE = 100


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
    _ctx: SearchConfig,
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
    return insight


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
    from tools.cq.search.pipeline.search_runtime import merge_language_matches
    from tools.cq.search.pipeline.smart_search_sections import build_sections
    from tools.cq.search.pipeline.smart_search_summary import build_search_summary

    enriched_matches = merge_language_matches(
        partition_results=partition_results,
        lang_scope=ctx.lang_scope,
    )
    summary_raw, all_diagnostics = build_search_summary(
        ctx,
        partition_results,
        enriched_matches,
        python_semantic_overview={},
        python_semantic_telemetry={},
        python_semantic_diagnostics=[],
    )
    summary = as_search_summary(summary_from_mapping(summary_raw))
    object_runtime = build_object_resolved_view(enriched_matches, query=ctx.query)
    summary = msgspec.structs.replace(
        summary,
        resolved_objects=len(object_runtime.view.summaries),
        resolved_occurrences=len(object_runtime.view.occurrences),
    )
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
    insight_neighborhood, neighborhood_notes, neighborhood_summary = (
        _build_tree_sitter_neighborhood_preview(
            ctx=ctx,
            partition_results=partition_results,
            sections=sections,
            inputs=_NeighborhoodPreviewInputs(
                primary_target_finding=primary_target_finding,
                definition_matches=definition_matches,
                has_target_candidates=bool(candidate_findings),
            ),
        )
    )
    summary = msgspec.structs.replace(summary, tree_sitter_neighborhood=neighborhood_summary)
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
    inputs.summary = msgspec.structs.replace(
        inputs.summary,
        front_door_insight=to_public_front_door_insight_dict(insight),
    )

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
        sections=tuple(inputs.sections),
        key_findings=tuple(_build_search_result_key_findings(inputs)),
        evidence=tuple(build_finding(m, ctx.root) for m in inputs.enriched_matches[:MAX_EVIDENCE]),
    )
    register_search_object_view(run_id=run.run_id, view=inputs.object_runtime.view)
    result = msgspec.structs.replace(
        result,
        summary=msgspec.structs.replace(
            result.summary,
            cache_backend=snapshot_backend_metrics(root=ctx.root),
        ),
    )
    cache_maintenance_payload = contract_to_builtins(
        maintenance_tick(get_cq_cache_backend(root=ctx.root))
    )
    result = msgspec.structs.replace(
        result,
        summary=msgspec.structs.replace(
            result.summary,
            cache_maintenance=(
                cache_maintenance_payload if isinstance(cache_maintenance_payload, dict) else {}
            ),
        ),
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
