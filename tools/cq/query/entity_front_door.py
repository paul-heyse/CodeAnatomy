"""Front-door insight assembly helpers for entity query results."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path

import msgspec

from tools.cq.core.front_door_assembly import mark_partial_for_missing_languages
from tools.cq.core.front_door_contracts import (
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightBudgetV1,
    InsightConfidenceV1,
    InsightDegradationV1,
    InsightNeighborhoodV1,
    InsightRiskCountersV1,
    InsightSliceV1,
)
from tools.cq.core.front_door_entity import (
    augment_insight_with_semantic,
    build_entity_insight,
)
from tools.cq.core.front_door_render import to_public_front_door_insight_dict
from tools.cq.core.front_door_risk import risk_from_counters
from tools.cq.core.schema import CqResult, Finding, update_result_summary
from tools.cq.core.semantic_contracts import (
    SemanticContractStateInputV1,
    SemanticProvider,
    derive_semantic_contract_state,
)
from tools.cq.core.snb_schema import SemanticNodeRefV1
from tools.cq.core.summary_contract import SummaryV1, build_semantic_telemetry
from tools.cq.core.types import QueryLanguage
from tools.cq.query.shared_utils import extract_missing_languages
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentRequest,
    enrich_with_language_semantics,
    infer_language_for_path,
    provider_for_language,
    semantic_runtime_enabled,
)


@dataclass(slots=True)
class EntitySemanticTelemetry:
    semantic_attempted: int = 0
    semantic_applied: int = 0
    semantic_failed: int = 0
    semantic_timed_out: int = 0
    semantic_provider: SemanticProvider = "none"
    py_attempted: int = 0
    py_applied: int = 0
    py_failed: int = 0
    py_timed_out: int = 0
    rust_attempted: int = 0
    rust_applied: int = 0
    rust_failed: int = 0
    rust_timed_out: int = 0
    reasons: list[str] = dataclass_field(default_factory=list)
    semantic_planes: dict[str, object] | None = None


@dataclass(slots=True)
class CandidateNeighborhood:
    primary_target: Finding | None
    candidates: list[Finding]
    neighborhood: InsightNeighborhoodV1
    confidence: InsightConfidenceV1


def attach_entity_front_door_insight(
    result: CqResult,
    *,
    relationship_detail_max_matches: int,
) -> CqResult:
    """Build and attach front-door insight card to an entity result.

    Returns:
        CqResult: Updated result with front-door insight and semantic telemetry.
    """
    mode_value = result.summary.mode
    if isinstance(mode_value, str) and mode_value == "pattern":
        return result

    neighborhood_data = _build_candidate_neighborhood(result)
    risk = risk_from_counters(
        InsightRiskCountersV1(
            callers=neighborhood_data.neighborhood.callers.total,
            callees=neighborhood_data.neighborhood.callees.total,
            closure_capture_count=neighborhood_data.neighborhood.hierarchy_or_scope.total,
        )
    )
    degradation = _build_degradation(result.summary)
    insight = build_entity_insight(
        EntityInsightBuildRequestV1(
            summary=result.summary,
            primary_target=neighborhood_data.primary_target,
            neighborhood=neighborhood_data.neighborhood,
            risk=risk,
            confidence=neighborhood_data.confidence,
            degradation=degradation,
            budget=InsightBudgetV1(top_candidates=3, preview_per_slice=5, semantic_targets=3),
        )
    )

    insight, telemetry = _run_entity_semantic(
        result,
        candidates=neighborhood_data.candidates,
        insight=insight,
        relationship_detail_max_matches=relationship_detail_max_matches,
    )
    insight = _apply_semantic_contract_state(insight, telemetry)

    missing = extract_missing_languages(result.summary)
    if missing:
        insight = mark_partial_for_missing_languages(insight, missing_languages=missing)

    updates: dict[str, object] = {
        "python_semantic_telemetry": build_semantic_telemetry(
            attempted=telemetry.py_attempted,
            applied=telemetry.py_applied,
            failed=telemetry.py_failed,
            timed_out=telemetry.py_timed_out,
        ),
        "rust_semantic_telemetry": build_semantic_telemetry(
            attempted=telemetry.rust_attempted,
            applied=telemetry.rust_applied,
            failed=telemetry.rust_failed,
            timed_out=telemetry.rust_timed_out,
        ),
        "front_door_insight": to_public_front_door_insight_dict(insight),
    }
    if telemetry.semantic_planes is not None:
        updates["semantic_planes"] = telemetry.semantic_planes
    return update_result_summary(result, updates)


def _build_candidate_neighborhood(result: CqResult) -> CandidateNeighborhood:
    definition_findings = [
        finding for finding in result.key_findings if finding.category == "definition"
    ]
    primary_target = definition_findings[0] if definition_findings else None
    candidates = definition_findings[:3]

    callers_total = sum(_detail_int(finding, "caller_count") for finding in candidates)
    caller_preview = tuple(
        _preview_node(finding, "caller")
        for finding in candidates
        if _detail_int(finding, "caller_count") > 0
    )
    callees_total = sum(_detail_int(finding, "callee_count") for finding in candidates)
    callee_preview = tuple(
        _preview_node(finding, "callee")
        for finding in candidates
        if _detail_int(finding, "callee_count") > 0
    )

    scope_values = {
        str(finding.details.get("enclosing_scope"))
        for finding in candidates
        if isinstance(finding.details.get("enclosing_scope"), str)
        and str(finding.details.get("enclosing_scope")) not in {"", "<module>"}
    }
    scope_preview = tuple(
        SemanticNodeRefV1(
            node_id=f"scope:{value}",
            kind="scope",
            name=value,
            display_label=value,
            file_path="",
        )
        for value in sorted(scope_values)
    )

    neighborhood = InsightNeighborhoodV1(
        callers=InsightSliceV1(
            total=callers_total,
            preview=caller_preview,
            availability="partial" if callers_total > 0 else "unavailable",
            source="heuristic",
        ),
        callees=InsightSliceV1(
            total=callees_total,
            preview=callee_preview,
            availability="partial" if callees_total > 0 else "unavailable",
            source="heuristic",
        ),
        references=InsightSliceV1(availability="unavailable", source="none"),
        hierarchy_or_scope=InsightSliceV1(
            total=len(scope_preview),
            preview=scope_preview,
            availability="partial" if scope_preview else "unavailable",
            source="heuristic",
        ),
    )

    confidence = _confidence_from_candidates(candidates)
    return CandidateNeighborhood(
        primary_target=primary_target,
        candidates=candidates,
        neighborhood=neighborhood,
        confidence=confidence,
    )


def _build_degradation(summary: SummaryV1) -> InsightDegradationV1:
    dropped = getattr(summary, "dropped_by_scope", None)
    scope_filter_status = "dropped" if isinstance(dropped, dict) and dropped else "none"
    notes: list[str] = []
    if isinstance(dropped, dict) and dropped:
        notes.append(f"dropped_by_scope={dropped}")
    return InsightDegradationV1(
        semantic="skipped",
        scan=(
            "timed_out"
            if bool(summary.timed_out)
            else "truncated"
            if bool(summary.truncated)
            else "ok"
        ),
        scope_filter=scope_filter_status,
        notes=tuple(notes),
    )


def _run_entity_semantic(
    result: CqResult,
    *,
    candidates: list[Finding],
    insight: FrontDoorInsightV1,
    relationship_detail_max_matches: int,
) -> tuple[FrontDoorInsightV1, EntitySemanticTelemetry]:
    telemetry = EntitySemanticTelemetry()
    summary_matches = result.summary.matches
    match_count = summary_matches if isinstance(summary_matches, int) else len(result.key_findings)
    runtime_semantic_enabled = semantic_runtime_enabled()
    run_entity_semantic = (
        runtime_semantic_enabled and match_count <= relationship_detail_max_matches
    )

    if not run_entity_semantic:
        _mark_entity_semantic_not_attempted(
            result,
            candidates=candidates,
            telemetry=telemetry,
            runtime_semantic_enabled=runtime_semantic_enabled,
        )
        return insight, telemetry

    for finding in candidates:
        insight = _apply_candidate_semantic(result, finding, insight, telemetry)
    return insight, telemetry


def _mark_entity_semantic_not_attempted(
    result: CqResult,
    *,
    candidates: list[Finding],
    telemetry: EntitySemanticTelemetry,
    runtime_semantic_enabled: bool,
) -> None:
    for finding in candidates:
        if finding.anchor is None:
            continue
        target_file = Path(result.run.root) / finding.anchor.file
        target_language = infer_language_for_path(target_file)
        if target_language not in {"python", "rust"}:
            continue
        telemetry.semantic_provider = provider_for_language(target_language)
        if runtime_semantic_enabled:
            telemetry.reasons.append("not_attempted_by_budget")
        else:
            telemetry.reasons.append("not_attempted_runtime_disabled")
        return


def _apply_candidate_semantic(
    result: CqResult,
    finding: Finding,
    insight: FrontDoorInsightV1,
    telemetry: EntitySemanticTelemetry,
) -> FrontDoorInsightV1:
    target_context = _resolve_semantic_target_context(result, finding)
    if target_context is None:
        return insight

    target_file, target_language = target_context
    anchor = finding.anchor
    if anchor is None:
        return insight
    _record_semantic_attempt(telemetry, target_language)

    semantic_outcome = enrich_with_language_semantics(
        LanguageSemanticEnrichmentRequest(
            language=target_language,
            mode="entity",
            root=Path(result.run.root),
            file_path=target_file,
            line=max(1, int(anchor.line)),
            col=int(anchor.col or 0),
            run_id=result.run.run_id,
            symbol_hint=(
                str(finding.details.get("name"))
                if isinstance(finding.details.get("name"), str)
                else None
            ),
        )
    )
    _record_semantic_timeout(telemetry, target_language, timed_out=semantic_outcome.timed_out)
    payload = semantic_outcome.payload

    if payload is None:
        _record_semantic_failure(telemetry, target_language)
        telemetry.reasons.append(
            semantic_outcome.failure_reason
            or ("request_timeout" if semantic_outcome.timed_out else "request_failed")
        )
        return insight

    _record_semantic_applied(telemetry, target_language)
    insight = augment_insight_with_semantic(insight, payload, preview_per_slice=5)
    semantic_planes = payload.get("semantic_planes")
    if isinstance(semantic_planes, dict):
        telemetry.semantic_planes = dict(semantic_planes)
    return insight


def _resolve_semantic_target_context(
    result: CqResult,
    finding: Finding,
) -> tuple[Path, QueryLanguage] | None:
    if finding.anchor is None:
        return None
    target_file = Path(result.run.root) / finding.anchor.file
    target_language = infer_language_for_path(target_file)
    if target_language not in {"python", "rust"}:
        return None
    return target_file, target_language


def _record_semantic_attempt(
    telemetry: EntitySemanticTelemetry, target_language: QueryLanguage
) -> None:
    if telemetry.semantic_provider == "none":
        telemetry.semantic_provider = provider_for_language(target_language)
    telemetry.semantic_attempted += 1
    if target_language == "python":
        telemetry.py_attempted += 1
    else:
        telemetry.rust_attempted += 1


def _record_semantic_timeout(
    telemetry: EntitySemanticTelemetry,
    target_language: QueryLanguage,
    *,
    timed_out: bool,
) -> None:
    timeout_count = int(timed_out)
    telemetry.semantic_timed_out += timeout_count
    if target_language == "python":
        telemetry.py_timed_out += timeout_count
    else:
        telemetry.rust_timed_out += timeout_count


def _record_semantic_failure(
    telemetry: EntitySemanticTelemetry, target_language: QueryLanguage
) -> None:
    telemetry.semantic_failed += 1
    if target_language == "python":
        telemetry.py_failed += 1
    else:
        telemetry.rust_failed += 1


def _record_semantic_applied(
    telemetry: EntitySemanticTelemetry, target_language: QueryLanguage
) -> None:
    telemetry.semantic_applied += 1
    if target_language == "python":
        telemetry.py_applied += 1
    else:
        telemetry.rust_applied += 1


def _apply_semantic_contract_state(
    insight: FrontDoorInsightV1,
    telemetry: EntitySemanticTelemetry,
) -> FrontDoorInsightV1:
    if telemetry.semantic_provider == "none":
        telemetry.reasons.append("provider_unavailable")
    elif (
        telemetry.semantic_attempted <= 0
        and "not_attempted_by_budget" not in telemetry.reasons
        and "not_attempted_runtime_disabled" not in telemetry.reasons
    ):
        telemetry.reasons.append("not_attempted_by_design")

    semantic_state = derive_semantic_contract_state(
        SemanticContractStateInputV1(
            provider=telemetry.semantic_provider,
            available=telemetry.semantic_provider != "none",
            attempted=telemetry.semantic_attempted,
            applied=telemetry.semantic_applied,
            failed=max(
                telemetry.semantic_failed, telemetry.semantic_attempted - telemetry.semantic_applied
            ),
            timed_out=telemetry.semantic_timed_out,
            reasons=tuple(dict.fromkeys(telemetry.reasons)),
        )
    )
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            semantic=semantic_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *semantic_state.reasons])),
        ),
    )


def _detail_int(finding: Finding, key: str) -> int:
    value = finding.details.get(key)
    return value if isinstance(value, int) else 0


def _preview_node(finding: Finding, suffix: str, label: str | None = None) -> SemanticNodeRefV1:
    anchor = finding.anchor
    name = str(finding.details.get("name") or finding.message)
    file_path = anchor.file if anchor else ""
    line = anchor.line if anchor else 0
    node_id = f"entity:{suffix}:{file_path}:{line}:{name}"
    return SemanticNodeRefV1(
        node_id=node_id,
        kind="definition",
        name=name,
        display_label=label or name,
        file_path=file_path,
    )


def _confidence_from_candidates(candidates: list[Finding]) -> InsightConfidenceV1:
    confidence = InsightConfidenceV1(evidence_kind="resolved_ast", score=0.8, bucket="high")
    for finding in candidates:
        score = finding.details.score
        if score is None:
            continue
        return InsightConfidenceV1(
            evidence_kind=score.evidence_kind or confidence.evidence_kind,
            score=float(score.confidence_score) if score.confidence_score is not None else 0.8,
            bucket=score.confidence_bucket or confidence.bucket,
        )
    return confidence


__all__ = ["attach_entity_front_door_insight"]
