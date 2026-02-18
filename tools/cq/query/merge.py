"""Query merge helper wrappers for auto-scope execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from tools.cq.core.contracts import MergeResultsRequest
from tools.cq.core.schema import CqResult, update_result_summary
from tools.cq.core.services import EntityFrontDoorRequest
from tools.cq.core.summary_types import SummaryEnvelopeV1, coerce_semantic_telemetry
from tools.cq.core.toolchain import Toolchain
from tools.cq.core.types import QueryLanguage
from tools.cq.orchestration.orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.query.ir import Query
from tools.cq.query.shared_utils import count_result_matches, extract_missing_languages
from tools.cq.search.semantic.diagnostics import (
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    features_from_query,
    is_python_oriented_query_ir,
)

if TYPE_CHECKING:
    from tools.cq.core.bootstrap import CqRuntimeServices


@dataclass(frozen=True)
class MergeAutoScopeQueryRequestV1:
    """Request contract for auto-scope query result merging."""

    query: Query
    results: dict[QueryLanguage, CqResult]
    root: Path
    argv: tuple[str, ...]
    tc: Toolchain
    summary_common: dict[str, object]
    services: CqRuntimeServices


def _merge_semantic_contract_inputs(
    summary: SummaryEnvelopeV1,
) -> tuple[str, bool, int, int, int, int]:
    py_telemetry = coerce_semantic_telemetry(summary.python_semantic_telemetry)
    rust_telemetry = coerce_semantic_telemetry(summary.rust_semantic_telemetry)
    py_attempted = py_telemetry.attempted if py_telemetry is not None else 0
    py_applied = py_telemetry.applied if py_telemetry is not None else 0
    py_failed = py_telemetry.failed if py_telemetry is not None else 0
    py_timed_out = py_telemetry.timed_out if py_telemetry is not None else 0
    rust_attempted = rust_telemetry.attempted if rust_telemetry is not None else 0
    rust_applied = rust_telemetry.applied if rust_telemetry is not None else 0
    rust_failed = rust_telemetry.failed if rust_telemetry is not None else 0
    rust_timed_out = rust_telemetry.timed_out if rust_telemetry is not None else 0
    provider = "none"
    if rust_attempted > 0 and py_attempted <= 0:
        provider = "rust_static"
    elif py_attempted > 0:
        provider = "python_static"
    elif rust_attempted > 0:
        provider = "rust_static"
    elif summary.lang_scope == "auto":
        provider = "python_static"
    attempted = py_attempted + rust_attempted
    applied = py_applied + rust_applied
    failed = py_failed + rust_failed
    timed_out = py_timed_out + rust_timed_out
    return provider, provider != "none", attempted, applied, failed, timed_out


def _mark_entity_insight_partial_from_summary(result: CqResult) -> CqResult:
    import msgspec

    from tools.cq.core.front_door_assembly import mark_partial_for_missing_languages
    from tools.cq.core.front_door_render import (
        coerce_front_door_insight,
        to_public_front_door_insight_dict,
    )
    from tools.cq.core.semantic_contracts import (
        SemanticContractStateInputV1,
        SemanticProvider,
        derive_semantic_contract_state,
    )

    insight = coerce_front_door_insight(result.summary.front_door_insight)
    if insight is None:
        return result
    lang_scope = result.summary.lang_scope
    if isinstance(lang_scope, str) and lang_scope == "auto":
        missing = extract_missing_languages(result.summary)
        if missing:
            insight = mark_partial_for_missing_languages(insight, missing_languages=missing)
    provider, available, attempted, applied, failed, timed_out = _merge_semantic_contract_inputs(
        result.summary
    )
    semantic_state = derive_semantic_contract_state(
        SemanticContractStateInputV1(
            provider=cast("SemanticProvider", provider),
            available=available,
            attempted=attempted,
            applied=applied,
            failed=max(failed, attempted - applied if attempted > applied else 0),
            timed_out=timed_out,
        )
    )
    insight = msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            semantic=semantic_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *semantic_state.reasons])),
        ),
    )
    return update_result_summary(
        result,
        {"front_door_insight": to_public_front_door_insight_dict(insight)},
    )


def merge_auto_scope_query_results(request: MergeAutoScopeQueryRequestV1) -> CqResult:
    """Merge per-language query results using canonical executor behavior.

    Returns:
        Merged CQ result using canonical auto-scope semantics.
    """
    query = request.query
    results = request.results
    diagnostics = build_cross_language_diagnostics(
        lang_scope=query.lang_scope,
        python_matches=count_result_matches(results.get("python")),
        rust_matches=count_result_matches(results.get("rust")),
        python_oriented=is_python_oriented_query_ir(query),
    )
    capability_diagnostics = build_capability_diagnostics(
        features=features_from_query(query),
        lang_scope=query.lang_scope,
    )
    diagnostics = list(diagnostics) + capability_diagnostics
    diagnostic_payloads = diagnostics_to_summary_payload(diagnostics)
    language_capabilities = build_language_capabilities(lang_scope=query.lang_scope)
    run = runmeta_for_scope_merge(
        macro="q",
        root=request.root,
        argv=list(request.argv),
        tc=request.tc,
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope=query.lang_scope,
            results=results,
            run=run,
            diagnostics=diagnostics,
            diagnostic_payloads=diagnostic_payloads,
            language_capabilities=language_capabilities,
            summary_common=request.summary_common,
        )
    )
    if query.is_pattern_query:
        return merged
    if merged.summary.front_door_insight is not None:
        return _mark_entity_insight_partial_from_summary(merged)
    return request.services.entity.attach_front_door(
        EntityFrontDoorRequest(result=merged),
    )


__all__ = ["MergeAutoScopeQueryRequestV1", "merge_auto_scope_query_results"]
