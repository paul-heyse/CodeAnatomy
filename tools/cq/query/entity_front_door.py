"""Front-door insight assembly helpers for entity query results."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path

import msgspec

from tools.cq.core.front_door_insight import (
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightBudgetV1,
    InsightConfidenceV1,
    InsightDegradationV1,
    InsightNeighborhoodV1,
    InsightRiskCountersV1,
    InsightSliceV1,
    augment_insight_with_lsp,
    build_entity_insight,
    mark_partial_for_missing_languages,
    risk_from_counters,
    to_public_front_door_insight_dict,
)
from tools.cq.core.schema import CqResult, Finding
from tools.cq.core.snb_schema import SemanticNodeRefV1
from tools.cq.query.language import QueryLanguage
from tools.cq.search.lsp_contract_state import (
    LspContractStateInputV1,
    LspProvider,
    derive_lsp_contract_state,
)
from tools.cq.search.lsp_front_door_adapter import (
    LanguageLspEnrichmentRequest,
    enrich_with_language_lsp,
    infer_language_for_path,
    lsp_runtime_enabled,
    provider_for_language,
)


@dataclass(slots=True)
class EntityLspTelemetry:
    lsp_attempted: int = 0
    lsp_applied: int = 0
    lsp_failed: int = 0
    lsp_timed_out: int = 0
    lsp_provider: LspProvider = "none"
    py_attempted: int = 0
    py_applied: int = 0
    py_failed: int = 0
    py_timed_out: int = 0
    rust_attempted: int = 0
    rust_applied: int = 0
    rust_failed: int = 0
    rust_timed_out: int = 0
    reasons: list[str] = dataclass_field(default_factory=list)


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
) -> None:
    """Build and attach front-door insight card to an entity result."""
    mode_value = result.summary.get("mode")
    if isinstance(mode_value, str) and mode_value == "pattern":
        return

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
            budget=InsightBudgetV1(top_candidates=3, preview_per_slice=5, lsp_targets=3),
        )
    )

    insight, telemetry = _run_entity_lsp(
        result,
        candidates=neighborhood_data.candidates,
        insight=insight,
        relationship_detail_max_matches=relationship_detail_max_matches,
    )
    insight = _apply_lsp_contract_state(insight, telemetry)

    missing = _missing_languages_from_summary(result.summary)
    if missing:
        insight = mark_partial_for_missing_languages(insight, missing_languages=missing)

    result.summary["pyrefly_telemetry"] = {
        "attempted": telemetry.py_attempted,
        "applied": telemetry.py_applied,
        "failed": max(telemetry.py_failed, telemetry.py_attempted - telemetry.py_applied),
        "skipped": 0,
        "timed_out": telemetry.py_timed_out,
    }
    result.summary["rust_lsp_telemetry"] = {
        "attempted": telemetry.rust_attempted,
        "applied": telemetry.rust_applied,
        "failed": max(telemetry.rust_failed, telemetry.rust_attempted - telemetry.rust_applied),
        "skipped": 0,
        "timed_out": telemetry.rust_timed_out,
    }
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)


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


def _build_degradation(summary: dict[str, object]) -> InsightDegradationV1:
    dropped = summary.get("dropped_by_scope")
    scope_filter_status = "dropped" if isinstance(dropped, dict) and dropped else "none"
    notes: list[str] = []
    if isinstance(dropped, dict) and dropped:
        notes.append(f"dropped_by_scope={dropped}")
    return InsightDegradationV1(
        lsp="skipped",
        scan=(
            "timed_out"
            if bool(summary.get("timed_out"))
            else "truncated"
            if bool(summary.get("truncated"))
            else "ok"
        ),
        scope_filter=scope_filter_status,
        notes=tuple(notes),
    )


def _run_entity_lsp(
    result: CqResult,
    *,
    candidates: list[Finding],
    insight: FrontDoorInsightV1,
    relationship_detail_max_matches: int,
) -> tuple[FrontDoorInsightV1, EntityLspTelemetry]:
    telemetry = EntityLspTelemetry()
    summary_matches = result.summary.get("matches")
    match_count = summary_matches if isinstance(summary_matches, int) else len(result.key_findings)
    runtime_lsp_enabled = lsp_runtime_enabled()
    run_entity_lsp = runtime_lsp_enabled and match_count <= relationship_detail_max_matches

    if not run_entity_lsp:
        _mark_entity_lsp_not_attempted(
            result,
            candidates=candidates,
            telemetry=telemetry,
            runtime_lsp_enabled=runtime_lsp_enabled,
        )
        return insight, telemetry

    for finding in candidates:
        insight = _apply_candidate_lsp(result, finding, insight, telemetry)
    return insight, telemetry


def _mark_entity_lsp_not_attempted(
    result: CqResult,
    *,
    candidates: list[Finding],
    telemetry: EntityLspTelemetry,
    runtime_lsp_enabled: bool,
) -> None:
    for finding in candidates:
        if finding.anchor is None:
            continue
        target_file = Path(result.run.root) / finding.anchor.file
        target_language = infer_language_for_path(target_file)
        if target_language not in {"python", "rust"}:
            continue
        telemetry.lsp_provider = provider_for_language(target_language)
        if runtime_lsp_enabled:
            telemetry.reasons.append("not_attempted_by_budget")
        else:
            telemetry.reasons.append("not_attempted_runtime_disabled")
        return


def _apply_candidate_lsp(
    result: CqResult,
    finding: Finding,
    insight: FrontDoorInsightV1,
    telemetry: EntityLspTelemetry,
) -> FrontDoorInsightV1:
    target_context = _resolve_lsp_target_context(result, finding)
    if target_context is None:
        return insight

    target_file, target_language = target_context
    anchor = finding.anchor
    if anchor is None:
        return insight
    _record_lsp_attempt(telemetry, target_language)

    lsp_outcome = enrich_with_language_lsp(
        LanguageLspEnrichmentRequest(
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
    _record_lsp_timeout(telemetry, target_language, timed_out=lsp_outcome.timed_out)
    payload = lsp_outcome.payload

    if payload is None:
        _record_lsp_failure(telemetry, target_language)
        telemetry.reasons.append(
            lsp_outcome.failure_reason
            or ("request_timeout" if lsp_outcome.timed_out else "request_failed")
        )
        return insight

    _record_lsp_applied(telemetry, target_language)
    insight = augment_insight_with_lsp(insight, payload, preview_per_slice=5)
    advanced_planes = payload.get("advanced_planes")
    if isinstance(advanced_planes, dict):
        result.summary["lsp_advanced_planes"] = dict(advanced_planes)
    return insight


def _resolve_lsp_target_context(
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


def _record_lsp_attempt(telemetry: EntityLspTelemetry, target_language: QueryLanguage) -> None:
    if telemetry.lsp_provider == "none":
        telemetry.lsp_provider = provider_for_language(target_language)
    telemetry.lsp_attempted += 1
    if target_language == "python":
        telemetry.py_attempted += 1
    else:
        telemetry.rust_attempted += 1


def _record_lsp_timeout(
    telemetry: EntityLspTelemetry,
    target_language: QueryLanguage,
    *,
    timed_out: bool,
) -> None:
    timeout_count = int(timed_out)
    telemetry.lsp_timed_out += timeout_count
    if target_language == "python":
        telemetry.py_timed_out += timeout_count
    else:
        telemetry.rust_timed_out += timeout_count


def _record_lsp_failure(telemetry: EntityLspTelemetry, target_language: QueryLanguage) -> None:
    telemetry.lsp_failed += 1
    if target_language == "python":
        telemetry.py_failed += 1
    else:
        telemetry.rust_failed += 1


def _record_lsp_applied(telemetry: EntityLspTelemetry, target_language: QueryLanguage) -> None:
    telemetry.lsp_applied += 1
    if target_language == "python":
        telemetry.py_applied += 1
    else:
        telemetry.rust_applied += 1


def _apply_lsp_contract_state(
    insight: FrontDoorInsightV1,
    telemetry: EntityLspTelemetry,
) -> FrontDoorInsightV1:
    if telemetry.lsp_provider == "none":
        telemetry.reasons.append("provider_unavailable")
    elif (
        telemetry.lsp_attempted <= 0
        and "not_attempted_by_budget" not in telemetry.reasons
        and "not_attempted_runtime_disabled" not in telemetry.reasons
    ):
        telemetry.reasons.append("not_attempted_by_design")

    lsp_state = derive_lsp_contract_state(
        LspContractStateInputV1(
            provider=telemetry.lsp_provider,
            available=telemetry.lsp_provider != "none",
            attempted=telemetry.lsp_attempted,
            applied=telemetry.lsp_applied,
            failed=max(telemetry.lsp_failed, telemetry.lsp_attempted - telemetry.lsp_applied),
            timed_out=telemetry.lsp_timed_out,
            reasons=tuple(dict.fromkeys(telemetry.reasons)),
        )
    )
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            lsp=lsp_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *lsp_state.reasons])),
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


def _missing_languages_from_summary(summary: dict[str, object]) -> list[str]:
    languages = summary.get("languages")
    if not isinstance(languages, dict):
        return []
    missing: list[str] = []
    for lang, payload in languages.items():
        lang_name = str(lang)
        if not isinstance(payload, dict):
            missing.append(lang_name)
            continue
        total = payload.get("total_matches")
        if isinstance(total, int):
            if total <= 0:
                missing.append(lang_name)
            continue
        matches = payload.get("matches")
        if isinstance(matches, int) and matches <= 0:
            missing.append(lang_name)
    return missing


__all__ = ["attach_entity_front_door_insight"]
