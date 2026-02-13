"""Front-door insight contract and helpers.

This module defines the canonical ``FrontDoorInsightV1`` schema used by
search, calls, and entity outputs. The schema is intentionally front-door
focused: concise target grounding, neighborhood previews, risk drivers,
confidence, degradation status, budgets, and artifact references.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Literal

import msgspec

from tools.cq.core.snb_schema import NeighborhoodSliceV1, SemanticNodeRefV1
from tools.cq.core.structs import CqStruct
from tools.cq.search.lsp_contract_state import (
    LspContractStateInputV1,
    LspStatus,
    derive_lsp_contract_state,
)

if TYPE_CHECKING:
    from tools.cq.core.schema import Finding

InsightSource = Literal["search", "calls", "entity"]
Availability = Literal["full", "partial", "unavailable"]
NeighborhoodSource = Literal["structural", "lsp", "heuristic", "none"]
RiskLevel = Literal["low", "med", "high"]

_DEFAULT_TOP_CANDIDATES = 3
_DEFAULT_PREVIEW_PER_SLICE = 5
_HIGH_CALLER_THRESHOLD = 10
_HIGH_CALLER_STRICT_THRESHOLD = 10
_MEDIUM_CALLER_THRESHOLD = 4
_MEDIUM_CALLER_STRICT_THRESHOLD = 3
_ARG_VARIANCE_THRESHOLD = 3
_FILES_WITH_CALLS_THRESHOLD = 3
_DEFAULT_LSP_TARGETS = 1


class InsightLocationV1(CqStruct, frozen=True):
    """Location payload for a selected target."""

    file: str = ""
    line: int | None = None
    col: int | None = None


class InsightTargetV1(CqStruct, frozen=True):
    """Primary target selected by a front-door command."""

    symbol: str
    kind: str = "unknown"
    location: InsightLocationV1 = InsightLocationV1()
    signature: str | None = None
    qualname: str | None = None
    selection_reason: str = ""


class InsightSliceV1(CqStruct, frozen=True):
    """Preview-able neighborhood slice with provenance and availability."""

    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    availability: Availability = "unavailable"
    source: NeighborhoodSource = "none"
    overflow_artifact_ref: str | None = None


class InsightNeighborhoodV1(CqStruct, frozen=True):
    """Neighborhood envelope used by the front-door card."""

    callers: InsightSliceV1 = InsightSliceV1()
    callees: InsightSliceV1 = InsightSliceV1()
    references: InsightSliceV1 = InsightSliceV1()
    hierarchy_or_scope: InsightSliceV1 = InsightSliceV1()


class InsightRiskCountersV1(CqStruct, frozen=True):
    """Deterministic risk counters for edit-surface evaluation."""

    callers: int = 0
    callees: int = 0
    files_with_calls: int = 0
    arg_shape_count: int = 0
    forwarding_count: int = 0
    hazard_count: int = 0
    closure_capture_count: int = 0


class InsightRiskV1(CqStruct, frozen=True):
    """Risk level + explicit drivers and counters."""

    level: RiskLevel = "low"
    drivers: tuple[str, ...] = ()
    counters: InsightRiskCountersV1 = InsightRiskCountersV1()


class InsightConfidenceV1(CqStruct, frozen=True):
    """Confidence payload used by card headline and machine parsing."""

    evidence_kind: str = "unknown"
    score: float = 0.0
    bucket: str = "low"


class InsightDegradationV1(CqStruct, frozen=True):
    """Compact degradation status for front-door rendering."""

    lsp: LspStatus = "unavailable"
    scan: str = "ok"
    scope_filter: str = "none"
    notes: tuple[str, ...] = ()


class InsightBudgetV1(CqStruct, frozen=True):
    """Budget knobs used to keep front-door output bounded."""

    top_candidates: int = _DEFAULT_TOP_CANDIDATES
    preview_per_slice: int = _DEFAULT_PREVIEW_PER_SLICE
    lsp_targets: int = _DEFAULT_LSP_TARGETS


class InsightArtifactRefsV1(CqStruct, frozen=True):
    """Artifact references for offloaded diagnostic/detail payloads."""

    diagnostics: str | None = None
    telemetry: str | None = None
    neighborhood_overflow: str | None = None


class FrontDoorInsightV1(CqStruct, frozen=True):
    """Canonical front-door insight schema for search/calls/entity."""

    source: InsightSource
    target: InsightTargetV1
    neighborhood: InsightNeighborhoodV1 = InsightNeighborhoodV1()
    risk: InsightRiskV1 = InsightRiskV1()
    confidence: InsightConfidenceV1 = InsightConfidenceV1()
    degradation: InsightDegradationV1 = InsightDegradationV1()
    budget: InsightBudgetV1 = InsightBudgetV1()
    artifact_refs: InsightArtifactRefsV1 = InsightArtifactRefsV1()
    schema_version: str = "cq.insight.v1"


class SearchInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for search insight assembly."""

    summary: dict[str, object]
    primary_target: Finding | None
    target_candidates: tuple[Finding, ...]
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None


class CallsInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for calls insight assembly."""

    function_name: str
    signature: str | None
    location: InsightLocationV1 | None
    neighborhood: InsightNeighborhoodV1
    files_with_calls: int
    arg_shape_count: int
    forwarding_count: int
    hazard_counts: dict[str, int]
    confidence: InsightConfidenceV1
    budget: InsightBudgetV1 | None = None
    degradation: InsightDegradationV1 | None = None


class EntityInsightBuildRequestV1(CqStruct, frozen=True):
    """Typed request contract for entity insight assembly."""

    summary: dict[str, object]
    primary_target: Finding | None
    neighborhood: InsightNeighborhoodV1 | None = None
    risk: InsightRiskV1 | None = None
    confidence: InsightConfidenceV1 | None = None
    degradation: InsightDegradationV1 | None = None
    budget: InsightBudgetV1 | None = None


def render_insight_card(insight: FrontDoorInsightV1) -> list[str]:
    """Render a compact markdown card from a front-door insight.

    Returns:
        Markdown lines representing the insight card.
    """
    lines = ["## Insight Card", _render_target_line(insight.target)]
    lines.extend(_render_neighborhood_lines(insight.neighborhood))
    lines.append(_render_risk_line(insight.risk))
    lines.append(_render_confidence_line(insight.confidence))
    lines.append(_render_degradation_line(insight.degradation))
    lines.append(_render_budget_line(insight.budget))
    artifact_refs_line = _render_artifact_refs_line(insight.artifact_refs)
    if artifact_refs_line is not None:
        lines.append(artifact_refs_line)
    lines.append("")
    return lines


def _render_target_line(target: InsightTargetV1) -> str:
    location = _format_target_location(target.location)
    target_parts = [f"**{target.symbol}**", f"({target.kind})", location]
    if target.signature:
        target_parts.append(f"`{target.signature}`")
    return f"- Target: {' '.join(part for part in target_parts if part)}"


def _format_target_location(location: InsightLocationV1) -> str:
    if not location.file:
        return ""
    if location.line is not None:
        return f"`{location.file}:{location.line}`"
    return f"`{location.file}`"


def _render_neighborhood_lines(neighborhood: InsightNeighborhoodV1) -> list[str]:
    lines = [
        (
            "- Neighborhood: "
            f"callers={neighborhood.callers.total}, "
            f"callees={neighborhood.callees.total}, "
            f"references={neighborhood.references.total}, "
            f"scope={neighborhood.hierarchy_or_scope.total}"
        )
    ]
    callers_preview = _preview_labels(neighborhood.callers.preview)
    if callers_preview:
        lines.append(f"  - Top callers: {', '.join(callers_preview)}")
    callees_preview = _preview_labels(neighborhood.callees.preview)
    if callees_preview:
        lines.append(f"  - Top callees: {', '.join(callees_preview)}")
    return lines


def _render_risk_line(risk: InsightRiskV1) -> str:
    driver_text = ", ".join(risk.drivers) if risk.drivers else "none"
    counters = risk.counters
    return (
        "- Risk: "
        f"level={risk.level}; "
        f"drivers={driver_text}; "
        f"callers={counters.callers}, "
        f"callees={counters.callees}, "
        f"hazards={counters.hazard_count}, "
        f"forwarding={counters.forwarding_count}"
    )


def _render_confidence_line(confidence: InsightConfidenceV1) -> str:
    return (
        "- Confidence: "
        f"evidence={confidence.evidence_kind}, "
        f"score={confidence.score:.2f}, "
        f"bucket={confidence.bucket}"
    )


def _render_degradation_line(degradation: InsightDegradationV1) -> str:
    notes = f" ({'; '.join(degradation.notes)})" if degradation.notes else ""
    return (
        "- Degradation: "
        f"lsp={degradation.lsp}, "
        f"scan={degradation.scan}, "
        f"scope_filter={degradation.scope_filter}{notes}"
    )


def _render_budget_line(budget: InsightBudgetV1) -> str:
    return (
        "- Budget: "
        f"top_candidates={budget.top_candidates}, "
        f"preview_per_slice={budget.preview_per_slice}, "
        f"lsp_targets={budget.lsp_targets}"
    )


def _render_artifact_refs_line(artifact_refs: InsightArtifactRefsV1) -> str | None:
    ref_parts = [
        f"diagnostics={artifact_refs.diagnostics}" if artifact_refs.diagnostics else None,
        f"telemetry={artifact_refs.telemetry}" if artifact_refs.telemetry else None,
        f"neighborhood_overflow={artifact_refs.neighborhood_overflow}"
        if artifact_refs.neighborhood_overflow
        else None,
    ]
    refs = [part for part in ref_parts if part is not None]
    if not refs:
        return None
    return f"- Artifact Refs: {' | '.join(refs)}"


def build_neighborhood_from_slices(
    slices: Sequence[NeighborhoodSliceV1],
    *,
    preview_per_slice: int = _DEFAULT_PREVIEW_PER_SLICE,
    source: NeighborhoodSource = "structural",
    overflow_artifact_ref: str | None = None,
) -> InsightNeighborhoodV1:
    """Map structural neighborhood slices into insight neighborhood schema.

    Returns:
        Insight neighborhood payload composed from matching slices.
    """
    callers = _collect_slice_group(
        slices,
        kinds={"callers"},
        preview_per_slice=preview_per_slice,
        source=source,
        overflow_artifact_ref=overflow_artifact_ref,
    )
    callees = _collect_slice_group(
        slices,
        kinds={"callees"},
        preview_per_slice=preview_per_slice,
        source=source,
        overflow_artifact_ref=overflow_artifact_ref,
    )
    references = _collect_slice_group(
        slices,
        kinds={"references", "imports", "importers"},
        preview_per_slice=preview_per_slice,
        source=source,
        overflow_artifact_ref=overflow_artifact_ref,
    )
    hierarchy = _collect_slice_group(
        slices,
        kinds={
            "parents",
            "children",
            "siblings",
            "enclosing_context",
            "implementations",
            "type_supertypes",
            "type_subtypes",
            "related",
        },
        preview_per_slice=preview_per_slice,
        source=source,
        overflow_artifact_ref=overflow_artifact_ref,
    )

    return InsightNeighborhoodV1(
        callers=callers,
        callees=callees,
        references=references,
        hierarchy_or_scope=hierarchy,
    )


def augment_insight_with_lsp(
    insight: FrontDoorInsightV1,
    lsp_payload: dict[str, object],
    *,
    preview_per_slice: int | None = None,
) -> FrontDoorInsightV1:
    """Overlay LSP data on top of an existing insight payload.

    Returns:
        Insight payload augmented with LSP-derived data.
    """
    limit = preview_per_slice or insight.budget.preview_per_slice
    neighborhood = insight.neighborhood

    call_graph = lsp_payload.get("call_graph")
    if isinstance(call_graph, dict):
        callers_preview = _node_refs_from_lsp_entries(call_graph.get("incoming_callers"), limit)
        callees_preview = _node_refs_from_lsp_entries(call_graph.get("outgoing_callees"), limit)
        callers_total = _read_total(call_graph.get("incoming_total"), fallback=len(callers_preview))
        callees_total = _read_total(call_graph.get("outgoing_total"), fallback=len(callees_preview))

        neighborhood = msgspec.structs.replace(
            neighborhood,
            callers=_merge_slice(
                neighborhood.callers,
                total=callers_total,
                preview=callers_preview,
                source="lsp",
            ),
            callees=_merge_slice(
                neighborhood.callees,
                total=callees_total,
                preview=callees_preview,
                source="lsp",
            ),
        )

    references_total = _read_reference_total(lsp_payload)
    if references_total is not None:
        neighborhood = msgspec.structs.replace(
            neighborhood,
            references=_merge_slice(
                neighborhood.references,
                total=references_total,
                preview=neighborhood.references.preview,
                source="lsp",
            ),
        )

    target = insight.target
    type_contract = lsp_payload.get("type_contract")
    if isinstance(type_contract, dict):
        signature = _string_or_none(type_contract.get("callable_signature"))
        resolved_type = _string_or_none(type_contract.get("resolved_type"))
        target = msgspec.structs.replace(
            target,
            signature=signature or resolved_type or target.signature,
        )

    confidence = msgspec.structs.replace(
        insight.confidence,
        evidence_kind=insight.confidence.evidence_kind
        if insight.confidence.evidence_kind != "unknown"
        else "resolved_lsp",
        score=max(insight.confidence.score, 0.8),
        bucket=_max_bucket(insight.confidence.bucket, "high"),
    )
    degradation = msgspec.structs.replace(insight.degradation, lsp="ok")

    return msgspec.structs.replace(
        insight,
        target=target,
        neighborhood=neighborhood,
        confidence=confidence,
        degradation=degradation,
    )


def build_search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build search front-door insight payload.

    Returns:
        Front-door insight payload for search macro results.
    """
    target = _target_from_finding(
        request.primary_target,
        fallback_symbol=_string_or_none(request.summary.get("query")) or "search target",
        fallback_kind="query",
        selection_reason=(
            "top_definition" if request.primary_target is not None else "fallback_query"
        ),
    )
    confidence = _confidence_from_findings(request.target_candidates)
    confidence = msgspec.structs.replace(
        confidence,
        evidence_kind=confidence.evidence_kind
        if confidence.evidence_kind != "unknown"
        else _string_or_none(request.summary.get("scan_method")) or "resolved_ast",
    )

    neighborhood = request.neighborhood or _empty_neighborhood()
    risk = request.risk
    if risk is None:
        risk = risk_from_counters(
            InsightRiskCountersV1(
                callers=neighborhood.callers.total,
                callees=neighborhood.callees.total,
            )
        )
    degradation = request.degradation or _degradation_from_summary(request.summary)
    budget = request.budget or _default_search_budget(target_candidate_count=len(request.target_candidates))

    return FrontDoorInsightV1(
        source="search",
        target=target,
        neighborhood=neighborhood,
        risk=risk,
        confidence=confidence,
        degradation=degradation,
        budget=budget,
    )


def build_calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build calls front-door insight payload.

    Returns:
        Front-door insight payload for calls macro results.
    """
    target = InsightTargetV1(
        symbol=request.function_name,
        kind="function",
        location=request.location or InsightLocationV1(),
        signature=request.signature,
        selection_reason="resolved_calls_target",
    )

    counters = InsightRiskCountersV1(
        callers=request.neighborhood.callers.total,
        callees=request.neighborhood.callees.total,
        files_with_calls=request.files_with_calls,
        arg_shape_count=request.arg_shape_count,
        forwarding_count=request.forwarding_count,
        hazard_count=sum(request.hazard_counts.values()),
    )
    risk = risk_from_counters(counters)
    if request.hazard_counts:
        drivers = tuple(sorted(request.hazard_counts.keys()))
        risk = msgspec.structs.replace(
            risk, drivers=tuple(dict.fromkeys([*risk.drivers, *drivers]))
        )

    return FrontDoorInsightV1(
        source="calls",
        target=target,
        neighborhood=request.neighborhood,
        risk=risk,
        confidence=request.confidence,
        degradation=request.degradation or InsightDegradationV1(),
        budget=request.budget or _default_calls_budget(),
    )


def build_entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build entity front-door insight payload.

    Returns:
        Front-door insight payload for entity query results.
    """
    target = _target_from_finding(
        request.primary_target,
        fallback_symbol=_string_or_none(request.summary.get("query"))
        or _string_or_none(request.summary.get("entity_kind"))
        or "entity target",
        fallback_kind=_string_or_none(request.summary.get("entity_kind")) or "entity",
        selection_reason=(
            "top_entity_result" if request.primary_target is not None else "fallback_query"
        ),
    )

    neighborhood = request.neighborhood or _empty_neighborhood()
    risk = request.risk
    if risk is None:
        risk = risk_from_counters(
            InsightRiskCountersV1(
                callers=neighborhood.callers.total,
                callees=neighborhood.callees.total,
            )
        )

    confidence = request.confidence
    if confidence is None:
        confidence = InsightConfidenceV1(evidence_kind="resolved_ast", score=0.8, bucket="high")

    return FrontDoorInsightV1(
        source="entity",
        target=target,
        neighborhood=neighborhood,
        risk=risk,
        confidence=confidence,
        degradation=request.degradation or InsightDegradationV1(),
        budget=request.budget or _default_entity_budget(),
    )


def _default_search_budget(*, target_candidate_count: int) -> InsightBudgetV1:
    top_candidates = (
        min(_DEFAULT_TOP_CANDIDATES, target_candidate_count)
        if target_candidate_count > 0
        else _DEFAULT_TOP_CANDIDATES
    )
    return InsightBudgetV1(
        top_candidates=top_candidates,
        preview_per_slice=_DEFAULT_PREVIEW_PER_SLICE,
        lsp_targets=1,
    )


def _default_calls_budget() -> InsightBudgetV1:
    return InsightBudgetV1(
        top_candidates=_DEFAULT_TOP_CANDIDATES,
        preview_per_slice=_DEFAULT_PREVIEW_PER_SLICE,
        lsp_targets=1,
    )


def _default_entity_budget() -> InsightBudgetV1:
    return InsightBudgetV1(
        top_candidates=_DEFAULT_TOP_CANDIDATES,
        preview_per_slice=_DEFAULT_PREVIEW_PER_SLICE,
        lsp_targets=3,
    )


def attach_artifact_refs(
    insight: FrontDoorInsightV1,
    *,
    diagnostics: str | None = None,
    telemetry: str | None = None,
    neighborhood_overflow: str | None = None,
) -> FrontDoorInsightV1:
    """Attach artifact refs to an existing insight object.

    Returns:
        Insight payload with updated artifact references.
    """
    refs = msgspec.structs.replace(
        insight.artifact_refs,
        diagnostics=diagnostics or insight.artifact_refs.diagnostics,
        telemetry=telemetry or insight.artifact_refs.telemetry,
        neighborhood_overflow=neighborhood_overflow or insight.artifact_refs.neighborhood_overflow,
    )
    return msgspec.structs.replace(insight, artifact_refs=refs)


def attach_neighborhood_overflow_ref(
    insight: FrontDoorInsightV1,
    *,
    overflow_ref: str,
) -> FrontDoorInsightV1:
    """Attach overflow artifact ref to truncated neighborhood slices.

    Returns:
        Insight payload with overflow references on truncated slices.
    """

    def _with_ref(slice_payload: InsightSliceV1) -> InsightSliceV1:
        if slice_payload.total <= len(slice_payload.preview):
            return slice_payload
        return msgspec.structs.replace(
            slice_payload,
            overflow_artifact_ref=overflow_ref,
        )

    neighborhood = msgspec.structs.replace(
        insight.neighborhood,
        callers=_with_ref(insight.neighborhood.callers),
        callees=_with_ref(insight.neighborhood.callees),
        references=_with_ref(insight.neighborhood.references),
        hierarchy_or_scope=_with_ref(insight.neighborhood.hierarchy_or_scope),
    )
    refs = msgspec.structs.replace(insight.artifact_refs, neighborhood_overflow=overflow_ref)
    return msgspec.structs.replace(insight, neighborhood=neighborhood, artifact_refs=refs)


def mark_partial_for_missing_languages(
    insight: FrontDoorInsightV1,
    *,
    missing_languages: Sequence[str],
) -> FrontDoorInsightV1:
    """Mark insight slices partial when language partitions are missing.

    Returns:
        Insight payload updated with partial availability/degradation notes.
    """
    missing = tuple(sorted({lang.strip() for lang in missing_languages if lang.strip()}))
    if not missing:
        return insight

    def _downgrade(slice_payload: InsightSliceV1) -> InsightSliceV1:
        availability: Availability
        if slice_payload.availability in {"unavailable", "full"}:
            availability = "partial"
        else:
            availability = slice_payload.availability
        return msgspec.structs.replace(slice_payload, availability=availability)

    neighborhood = msgspec.structs.replace(
        insight.neighborhood,
        callers=_downgrade(insight.neighborhood.callers),
        callees=_downgrade(insight.neighborhood.callees),
        references=_downgrade(insight.neighborhood.references),
        hierarchy_or_scope=_downgrade(insight.neighborhood.hierarchy_or_scope),
    )
    note = f"missing_languages={','.join(missing)}"
    notes = tuple(dict.fromkeys([*insight.degradation.notes, note]))
    degradation = msgspec.structs.replace(
        insight.degradation,
        scope_filter="partial",
        notes=notes,
    )
    return msgspec.structs.replace(insight, neighborhood=neighborhood, degradation=degradation)


def coerce_front_door_insight(payload: object) -> FrontDoorInsightV1 | None:
    """Best-effort conversion from summary payload to insight struct.

    Returns:
        Parsed insight payload when conversion succeeds, otherwise `None`.
    """
    if isinstance(payload, FrontDoorInsightV1):
        return payload
    if not isinstance(payload, dict):
        return None
    try:
        return msgspec.convert(payload, FrontDoorInsightV1)
    except (TypeError, msgspec.ValidationError):
        return None


def risk_from_counters(counters: InsightRiskCountersV1) -> InsightRiskV1:
    """Build risk payload from deterministic counters.

    Returns:
        Risk payload with level and driver labels.
    """
    drivers: list[str] = []
    if counters.callers >= _HIGH_CALLER_THRESHOLD:
        drivers.append("high_call_surface")
    elif counters.callers >= _MEDIUM_CALLER_THRESHOLD:
        drivers.append("medium_call_surface")

    if counters.forwarding_count > 0:
        drivers.append("argument_forwarding")

    if counters.hazard_count > 0:
        drivers.append("dynamic_hazards")

    if counters.arg_shape_count > _ARG_VARIANCE_THRESHOLD:
        drivers.append("arg_shape_variance")

    if counters.closure_capture_count > 0:
        drivers.append("closure_capture")

    level = _risk_level_from_counters(counters)
    return InsightRiskV1(level=level, drivers=tuple(drivers), counters=counters)


def _risk_level_from_counters(counters: InsightRiskCountersV1) -> RiskLevel:
    if (
        counters.callers > _HIGH_CALLER_STRICT_THRESHOLD
        or counters.hazard_count > 0
        or (counters.forwarding_count > 0 and counters.callers > 0)
    ):
        return "high"
    if (
        counters.callers > _MEDIUM_CALLER_STRICT_THRESHOLD
        or counters.arg_shape_count > _ARG_VARIANCE_THRESHOLD
        or counters.files_with_calls > _FILES_WITH_CALLS_THRESHOLD
        or counters.closure_capture_count > 0
    ):
        return "med"
    return "low"


def _confidence_from_findings(findings: Sequence[Finding]) -> InsightConfidenceV1:
    best_score = 0.0
    best_bucket = "low"
    evidence_kind = "unknown"
    for finding in findings:
        score = finding.details.score
        if score is None:
            continue
        if score.confidence_score is not None and score.confidence_score > best_score:
            best_score = score.confidence_score
        if score.confidence_bucket:
            best_bucket = _max_bucket(best_bucket, score.confidence_bucket)
        if score.evidence_kind and evidence_kind == "unknown":
            evidence_kind = score.evidence_kind
    return InsightConfidenceV1(
        evidence_kind=evidence_kind,
        score=best_score,
        bucket=best_bucket,
    )


def _degradation_from_summary(summary: dict[str, object]) -> InsightDegradationV1:
    provider, lsp_available = _lsp_provider_and_availability(summary)
    attempted, applied, failed, timed_out = _read_lsp_telemetry(summary)
    lsp_reasons: list[str] = []
    if not lsp_available:
        lsp_reasons.append("provider_unavailable")
    elif attempted <= 0:
        lsp_reasons.append("not_attempted_by_design")
    elif applied <= 0:
        if timed_out > 0:
            lsp_reasons.append("request_timeout")
        if failed > 0:
            lsp_reasons.append("request_failed")
    lsp_state = derive_lsp_contract_state(
        LspContractStateInputV1(
            provider=provider,
            available=lsp_available,
            attempted=attempted,
            applied=applied,
            failed=failed,
            timed_out=timed_out,
            reasons=tuple(dict.fromkeys(lsp_reasons)),
        )
    )

    scan = "ok"
    if bool(summary.get("timed_out")):
        scan = "timed_out"
    elif bool(summary.get("truncated")):
        scan = "truncated"

    scope_filter = "none"
    dropped = summary.get("dropped_by_scope")
    if isinstance(dropped, dict) and dropped:
        scope_filter = "dropped"

    notes: list[str] = []
    if isinstance(dropped, dict) and dropped:
        notes.append(f"dropped_by_scope={dropped}")

    return InsightDegradationV1(
        lsp=lsp_state.status,
        scan=scan,
        scope_filter=scope_filter,
        notes=tuple(dict.fromkeys([*notes, *lsp_state.reasons])),
    )


def _target_from_finding(
    finding: Finding | None,
    *,
    fallback_symbol: str,
    fallback_kind: str,
    selection_reason: str,
) -> InsightTargetV1:
    if finding is None:
        return InsightTargetV1(
            symbol=fallback_symbol,
            kind=fallback_kind,
            selection_reason=selection_reason,
        )

    symbol = (
        _string_or_none(finding.details.get("name"))
        or _string_or_none(finding.details.get("match_text"))
        or _extract_symbol_from_message(finding.message)
        or fallback_symbol
    )
    kind = _string_or_none(finding.details.get("kind")) or finding.category or fallback_kind
    anchor = finding.anchor
    location = InsightLocationV1(
        file=anchor.file if anchor else "",
        line=anchor.line if anchor else None,
        col=anchor.col if anchor else None,
    )
    signature = _string_or_none(finding.details.get("signature"))
    qualname = _string_or_none(finding.details.get("qualified_name"))
    return InsightTargetV1(
        symbol=symbol,
        kind=kind,
        location=location,
        signature=signature,
        qualname=qualname,
        selection_reason=selection_reason,
    )


def _collect_slice_group(
    slices: Sequence[NeighborhoodSliceV1],
    *,
    kinds: set[str],
    preview_per_slice: int,
    source: NeighborhoodSource,
    overflow_artifact_ref: str | None,
) -> InsightSliceV1:
    group = [s for s in slices if s.kind in kinds]
    if not group:
        return InsightSliceV1(availability="unavailable", source="none")

    total = sum(s.total for s in group)
    preview: list[SemanticNodeRefV1] = []
    seen_ids: set[str] = set()
    for slice_item in group:
        for node in slice_item.preview:
            if node.node_id in seen_ids:
                continue
            seen_ids.add(node.node_id)
            preview.append(node)
            if len(preview) >= preview_per_slice:
                break
        if len(preview) >= preview_per_slice:
            break

    availability: Availability = "full" if total > 0 else "partial"
    overflow_ref = overflow_artifact_ref if total > len(preview) else None
    return InsightSliceV1(
        total=total,
        preview=tuple(preview),
        availability=availability,
        source=source,
        overflow_artifact_ref=overflow_ref,
    )


def _merge_slice(
    original: InsightSliceV1,
    *,
    total: int,
    preview: tuple[SemanticNodeRefV1, ...],
    source: NeighborhoodSource,
) -> InsightSliceV1:
    merged_total = max(original.total, total)
    merged_preview = preview or original.preview
    availability: Availability = "full" if merged_total > 0 else "partial"
    return InsightSliceV1(
        total=merged_total,
        preview=merged_preview,
        availability=availability,
        source=source,
        overflow_artifact_ref=original.overflow_artifact_ref,
    )


def _node_refs_from_lsp_entries(payload: object, limit: int) -> tuple[SemanticNodeRefV1, ...]:
    if not isinstance(payload, list):
        return ()
    refs: list[SemanticNodeRefV1] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        name = _string_or_none(entry.get("name")) or "unknown"
        file_path = _string_or_none(entry.get("file")) or _string_or_none(entry.get("uri")) or ""
        kind = _string_or_none(entry.get("kind")) or "function"
        refs.append(
            SemanticNodeRefV1(
                node_id=f"lsp:{file_path}:{name}",
                kind=kind,
                name=name,
                display_label=name,
                file_path=file_path,
            )
        )
        if len(refs) >= limit:
            break
    return tuple(refs)


def _read_reference_total(payload: dict[str, object]) -> int | None:
    local_scope = payload.get("local_scope_context")
    if not isinstance(local_scope, dict):
        symbol_grounding = payload.get("symbol_grounding")
        if isinstance(symbol_grounding, dict):
            refs = symbol_grounding.get("references")
            if isinstance(refs, list):
                return len([item for item in refs if isinstance(item, dict)])
        return None
    refs = local_scope.get("reference_locations")
    if isinstance(refs, list):
        return len([item for item in refs if isinstance(item, dict)])
    return None


def _extract_symbol_from_message(message: str) -> str | None:
    text = message.strip()
    if not text:
        return None
    if ":" in text:
        candidate = text.rsplit(":", maxsplit=1)[1].strip()
        return candidate or None
    return text


def _preview_labels(nodes: Iterable[SemanticNodeRefV1]) -> list[str]:
    labels: list[str] = [node.display_label or node.name for node in nodes]
    return labels[:3]


def _max_bucket(lhs: str, rhs: str) -> str:
    order = {"none": 0, "low": 1, "med": 2, "high": 3}
    return lhs if order.get(lhs, 0) >= order.get(rhs, 0) else rhs


def _string_or_none(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _read_total(value: object, *, fallback: int) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else fallback


def _empty_neighborhood() -> InsightNeighborhoodV1:
    return InsightNeighborhoodV1(
        callers=InsightSliceV1(availability="unavailable", source="none"),
        callees=InsightSliceV1(availability="unavailable", source="none"),
        references=InsightSliceV1(availability="unavailable", source="none"),
        hierarchy_or_scope=InsightSliceV1(availability="unavailable", source="none"),
    )


def to_public_front_door_insight_dict(insight: FrontDoorInsightV1) -> dict[str, object]:
    """Serialize insight with explicit/full schema fields for public JSON output.

    Returns:
        Public JSON-compatible dictionary representation of insight.
    """
    return {
        "source": insight.source,
        "schema_version": insight.schema_version,
        "target": _serialize_target(insight.target),
        "neighborhood": {
            "callers": _serialize_slice(insight.neighborhood.callers),
            "callees": _serialize_slice(insight.neighborhood.callees),
            "references": _serialize_slice(insight.neighborhood.references),
            "hierarchy_or_scope": _serialize_slice(insight.neighborhood.hierarchy_or_scope),
        },
        "risk": {
            "level": insight.risk.level,
            "drivers": list(insight.risk.drivers),
            "counters": _serialize_risk_counters(insight.risk.counters),
        },
        "confidence": {
            "evidence_kind": insight.confidence.evidence_kind,
            "score": float(insight.confidence.score),
            "bucket": insight.confidence.bucket,
        },
        "degradation": {
            "lsp": insight.degradation.lsp,
            "scan": insight.degradation.scan,
            "scope_filter": insight.degradation.scope_filter,
            "notes": list(insight.degradation.notes),
        },
        "budget": {
            "top_candidates": int(insight.budget.top_candidates),
            "preview_per_slice": int(insight.budget.preview_per_slice),
            "lsp_targets": int(insight.budget.lsp_targets),
        },
        "artifact_refs": {
            "diagnostics": insight.artifact_refs.diagnostics,
            "telemetry": insight.artifact_refs.telemetry,
            "neighborhood_overflow": insight.artifact_refs.neighborhood_overflow,
        },
    }


def _serialize_target(target: InsightTargetV1) -> dict[str, object]:
    return {
        "symbol": target.symbol,
        "kind": target.kind,
        "location": {
            "file": target.location.file,
            "line": target.location.line,
            "col": target.location.col,
        },
        "signature": target.signature,
        "qualname": target.qualname,
        "selection_reason": target.selection_reason,
    }


def _serialize_slice(slice_payload: InsightSliceV1) -> dict[str, object]:
    return {
        "total": int(slice_payload.total),
        "preview": [msgspec.to_builtins(node) for node in slice_payload.preview],
        "availability": slice_payload.availability,
        "source": slice_payload.source,
        "overflow_artifact_ref": slice_payload.overflow_artifact_ref,
    }


def _serialize_risk_counters(counters: InsightRiskCountersV1) -> dict[str, int]:
    return {
        "callers": int(counters.callers),
        "callees": int(counters.callees),
        "files_with_calls": int(counters.files_with_calls),
        "arg_shape_count": int(counters.arg_shape_count),
        "forwarding_count": int(counters.forwarding_count),
        "hazard_count": int(counters.hazard_count),
        "closure_capture_count": int(counters.closure_capture_count),
    }


def _lsp_provider_and_availability(
    summary: dict[str, object],
) -> tuple[Literal["pyrefly", "rust_analyzer", "none"], bool]:
    py_attempted = 0
    rust_attempted = 0
    py_telemetry = summary.get("pyrefly_telemetry")
    if isinstance(py_telemetry, dict):
        py_attempted = _int_or_none(py_telemetry.get("attempted")) or 0
    rust_telemetry = summary.get("rust_lsp_telemetry")
    if isinstance(rust_telemetry, dict):
        rust_attempted = _int_or_none(rust_telemetry.get("attempted")) or 0
    if rust_attempted > 0 and py_attempted <= 0:
        return "rust_analyzer", True
    if py_attempted > 0:
        return "pyrefly", True

    scope = _string_or_none(summary.get("lang_scope")) or "auto"
    order_raw = summary.get("language_order")
    order: tuple[str, ...]
    if isinstance(order_raw, list):
        order = tuple(str(item) for item in order_raw)
    else:
        order = ("python", "rust") if scope == "auto" else (scope,)

    has_python = "python" in order
    has_rust = "rust" in order
    if has_python:
        return "pyrefly", True
    if has_rust:
        return "rust_analyzer", True
    return "none", False


def _read_lsp_telemetry(summary: dict[str, object]) -> tuple[int, int, int, int]:
    attempted = 0
    applied = 0
    failed = 0
    timed_out = 0
    for key in ("pyrefly_telemetry", "rust_lsp_telemetry"):
        telemetry = summary.get(key)
        if not isinstance(telemetry, dict):
            continue
        attempted += _int_or_none(telemetry.get("attempted")) or 0
        applied += _int_or_none(telemetry.get("applied")) or 0
        failed += _int_or_none(telemetry.get("failed")) or 0
        timed_out += _int_or_none(telemetry.get("timed_out")) or 0
    return attempted, applied, failed, timed_out


__all__ = [
    "Availability",
    "CallsInsightBuildRequestV1",
    "EntityInsightBuildRequestV1",
    "FrontDoorInsightV1",
    "InsightArtifactRefsV1",
    "InsightBudgetV1",
    "InsightConfidenceV1",
    "InsightDegradationV1",
    "InsightLocationV1",
    "InsightNeighborhoodV1",
    "InsightRiskCountersV1",
    "InsightRiskV1",
    "InsightSliceV1",
    "InsightSource",
    "InsightTargetV1",
    "LspStatus",
    "NeighborhoodSource",
    "RiskLevel",
    "SearchInsightBuildRequestV1",
    "attach_artifact_refs",
    "attach_neighborhood_overflow_ref",
    "augment_insight_with_lsp",
    "build_calls_insight",
    "build_entity_insight",
    "build_neighborhood_from_slices",
    "build_search_insight",
    "coerce_front_door_insight",
    "mark_partial_for_missing_languages",
    "render_insight_card",
    "risk_from_counters",
    "to_public_front_door_insight_dict",
]
