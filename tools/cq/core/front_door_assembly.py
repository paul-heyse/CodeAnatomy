"""Front-door insight assembly helpers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

import msgspec

from tools.cq.core.front_door_contracts import (
    DEFAULT_INSIGHT_THRESHOLDS,
    Availability,
    CallsInsightBuildRequestV1,
    EntityInsightBuildRequestV1,
    FrontDoorInsightV1,
    InsightArtifactRefsV1,
    InsightBudgetV1,
    InsightConfidenceV1,
    InsightDegradationV1,
    InsightLocationV1,
    InsightNeighborhoodV1,
    InsightRiskCountersV1,
    InsightRiskV1,
    InsightSliceV1,
    InsightSource,
    InsightTargetV1,
    InsightThresholdPolicyV1,
    NeighborhoodSource,
    SearchInsightBuildRequestV1,
    SummaryLike,
)
from tools.cq.core.front_door_risk import risk_from_counters
from tools.cq.core.render_utils import summary_value
from tools.cq.core.semantic_contracts import (
    SemanticContractStateInputV1,
    SemanticStatus,
    derive_semantic_contract_state,
)
from tools.cq.core.snb_schema import NeighborhoodSliceV1, SemanticNodeRefV1
from tools.cq.core.summary_types import coerce_semantic_telemetry

if TYPE_CHECKING:
    from tools.cq.core.schema import Finding


def build_neighborhood_from_slices(
    slices: Sequence[NeighborhoodSliceV1],
    *,
    preview_per_slice: int = DEFAULT_INSIGHT_THRESHOLDS.default_preview_per_slice,
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


def augment_insight_with_semantic(
    insight: FrontDoorInsightV1,
    semantic_payload: dict[str, object],
    *,
    preview_per_slice: int | None = None,
) -> FrontDoorInsightV1:
    """Overlay static semantic data via extracted entity front-door module.

    Returns:
        Front-door insight payload with semantic overlays applied.
    """
    from tools.cq.core.front_door_entity import augment_insight_with_semantic as impl

    return impl(
        insight,
        semantic_payload,
        preview_per_slice=preview_per_slice,
    )


def build_search_insight(request: SearchInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build search front-door insight via extracted search module.

    Returns:
        Search front-door insight payload.
    """
    from tools.cq.core.front_door_search import build_search_insight as impl

    return impl(request)


def build_calls_insight(request: CallsInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build calls front-door insight via extracted calls module.

    Returns:
        Calls front-door insight payload.
    """
    from tools.cq.core.front_door_calls import build_calls_insight as impl

    return impl(request)


def build_entity_insight(request: EntityInsightBuildRequestV1) -> FrontDoorInsightV1:
    """Build entity front-door insight via extracted entity module.

    Returns:
        Entity front-door insight payload.
    """
    from tools.cq.core.front_door_entity import build_entity_insight as impl

    return impl(request)


def _default_search_budget(*, target_candidate_count: int) -> InsightBudgetV1:
    top_candidates = (
        min(DEFAULT_INSIGHT_THRESHOLDS.default_top_candidates, target_candidate_count)
        if target_candidate_count > 0
        else DEFAULT_INSIGHT_THRESHOLDS.default_top_candidates
    )
    return InsightBudgetV1(
        top_candidates=top_candidates,
        preview_per_slice=DEFAULT_INSIGHT_THRESHOLDS.default_preview_per_slice,
        semantic_targets=1,
    )


def _default_calls_budget() -> InsightBudgetV1:
    return InsightBudgetV1(
        top_candidates=DEFAULT_INSIGHT_THRESHOLDS.default_top_candidates,
        preview_per_slice=DEFAULT_INSIGHT_THRESHOLDS.default_preview_per_slice,
        semantic_targets=1,
    )


def _default_entity_budget() -> InsightBudgetV1:
    return InsightBudgetV1(
        top_candidates=DEFAULT_INSIGHT_THRESHOLDS.default_top_candidates,
        preview_per_slice=DEFAULT_INSIGHT_THRESHOLDS.default_preview_per_slice,
        semantic_targets=3,
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


def _degradation_from_summary(summary: SummaryLike) -> InsightDegradationV1:
    provider, semantic_available = _semantic_provider_and_availability(summary)
    attempted, applied, failed, timed_out = _read_semantic_telemetry(summary)
    semantic_reasons: list[str] = []
    if not semantic_available:
        semantic_reasons.append("provider_unavailable")
    elif attempted <= 0:
        semantic_reasons.append("not_attempted_by_design")
    elif applied <= 0:
        if timed_out > 0:
            semantic_reasons.append("request_timeout")
        if failed > 0:
            semantic_reasons.append("request_failed")
    semantic_state = derive_semantic_contract_state(
        SemanticContractStateInputV1(
            provider=provider,
            available=semantic_available,
            attempted=attempted,
            applied=applied,
            failed=failed,
            timed_out=timed_out,
            reasons=tuple(dict.fromkeys(semantic_reasons)),
        )
    )

    scan = "ok"
    if bool(summary_value(summary, "timed_out")):
        scan = "timed_out"
    elif bool(summary_value(summary, "truncated")):
        scan = "truncated"

    scope_filter = "none"
    dropped = summary_value(summary, "dropped_by_scope")
    if isinstance(dropped, dict) and dropped:
        scope_filter = "dropped"

    notes: list[str] = []
    if isinstance(dropped, dict) and dropped:
        notes.append(f"dropped_by_scope={dropped}")

    return InsightDegradationV1(
        semantic=semantic_state.status,
        scan=scan,
        scope_filter=scope_filter,
        notes=tuple(dict.fromkeys([*notes, *semantic_state.reasons])),
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


def _node_refs_from_semantic_entries(
    payload: object,
    limit: int,
) -> tuple[SemanticNodeRefV1, ...]:
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
                node_id=f"semantic:{file_path}:{name}",
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


def _semantic_provider_and_availability(
    summary: SummaryLike,
) -> tuple[Literal["python_static", "rust_static", "none"], bool]:
    py_attempted = 0
    rust_attempted = 0
    py_telemetry = coerce_semantic_telemetry(summary_value(summary, "python_semantic_telemetry"))
    if py_telemetry is not None:
        py_attempted = py_telemetry.attempted
    rust_telemetry = coerce_semantic_telemetry(summary_value(summary, "rust_semantic_telemetry"))
    if rust_telemetry is not None:
        rust_attempted = rust_telemetry.attempted
    if rust_attempted > 0 and py_attempted <= 0:
        return "rust_static", True
    if py_attempted > 0:
        return "python_static", True

    scope = _string_or_none(summary_value(summary, "lang_scope")) or "auto"
    order_raw = summary_value(summary, "language_order")
    order: tuple[str, ...]
    if isinstance(order_raw, list):
        order = tuple(str(item) for item in order_raw)
    else:
        order = ("python", "rust") if scope == "auto" else (scope,)

    has_python = "python" in order
    has_rust = "rust" in order
    if has_python:
        return "python_static", True
    if has_rust:
        return "rust_static", True
    return "none", False


def _read_semantic_telemetry(summary: SummaryLike) -> tuple[int, int, int, int]:
    attempted = 0
    applied = 0
    failed = 0
    timed_out = 0
    for key in ("python_semantic_telemetry", "rust_semantic_telemetry"):
        telemetry = coerce_semantic_telemetry(summary_value(summary, key))
        if telemetry is None:
            continue
        attempted += telemetry.attempted
        applied += telemetry.applied
        failed += telemetry.failed
        timed_out += telemetry.timed_out
    return attempted, applied, failed, timed_out


def default_search_budget(*, target_candidate_count: int) -> InsightBudgetV1:
    """Public wrapper for default search budget policy.

    Returns:
        InsightBudgetV1: Budget tuned to search target-cardinality.
    """
    return _default_search_budget(target_candidate_count=target_candidate_count)


def default_calls_budget() -> InsightBudgetV1:
    """Public wrapper for default calls budget policy.

    Returns:
        InsightBudgetV1: Standard calls insight budget defaults.
    """
    return _default_calls_budget()


def default_entity_budget() -> InsightBudgetV1:
    """Public wrapper for default entity budget policy.

    Returns:
        InsightBudgetV1: Standard entity insight budget defaults.
    """
    return _default_entity_budget()


def target_from_finding(
    finding: Finding | None,
    *,
    fallback_symbol: str,
    fallback_kind: str,
    selection_reason: str,
) -> InsightTargetV1:
    """Public wrapper for target extraction from findings.

    Returns:
        InsightTargetV1: Best target candidate with fallback resolution metadata.
    """
    return _target_from_finding(
        finding,
        fallback_symbol=fallback_symbol,
        fallback_kind=fallback_kind,
        selection_reason=selection_reason,
    )


def string_or_none(value: object) -> str | None:
    """Public wrapper for non-empty string coercion.

    Returns:
        str | None: Non-empty trimmed string, otherwise None.
    """
    return _string_or_none(value)


def confidence_from_findings(findings: Sequence[Finding]) -> InsightConfidenceV1:
    """Public wrapper for confidence aggregation from findings.

    Returns:
        InsightConfidenceV1: Confidence bucket/score inferred from finding set.
    """
    return _confidence_from_findings(findings)


def degradation_from_summary(summary: SummaryLike) -> InsightDegradationV1:
    """Public wrapper for degradation derivation from summary payload.

    Returns:
        InsightDegradationV1: Degradation state derived from summary telemetry.
    """
    return _degradation_from_summary(summary)


def empty_neighborhood() -> InsightNeighborhoodV1:
    """Public wrapper for an unavailable neighborhood payload.

    Returns:
        InsightNeighborhoodV1: Placeholder neighborhood with unavailable slices.
    """
    return _empty_neighborhood()


def node_refs_from_semantic_entries(
    payload: object,
    limit: int,
) -> tuple[SemanticNodeRefV1, ...]:
    """Public wrapper for semantic node-ref preview extraction.

    Returns:
        tuple[SemanticNodeRefV1, ...]: Truncated semantic node-reference preview.
    """
    return _node_refs_from_semantic_entries(payload, limit)


def read_total(value: object, *, fallback: int) -> int:
    """Public wrapper for integer-total coercion with fallback.

    Returns:
        int: Parsed total count or fallback when coercion fails.
    """
    return _read_total(value, fallback=fallback)


def merge_slice(
    original: InsightSliceV1,
    *,
    total: int,
    preview: tuple[SemanticNodeRefV1, ...],
    source: NeighborhoodSource,
) -> InsightSliceV1:
    """Public wrapper for slice merge policy.

    Returns:
        InsightSliceV1: Slice merged with semantic totals/preview/source metadata.
    """
    return _merge_slice(original, total=total, preview=preview, source=source)


def read_reference_total(payload: dict[str, object]) -> int | None:
    """Public wrapper for reference-total extraction.

    Returns:
        int | None: Reference total when present and coercible.
    """
    return _read_reference_total(payload)


def max_bucket(lhs: str, rhs: str) -> str:
    """Public wrapper for confidence bucket comparison.

    Returns:
        str: Higher-confidence bucket between the two inputs.
    """
    return _max_bucket(lhs, rhs)


__all__ = [
    "DEFAULT_INSIGHT_THRESHOLDS",
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
    "InsightThresholdPolicyV1",
    "NeighborhoodSource",
    "SearchInsightBuildRequestV1",
    "SemanticStatus",
    "attach_artifact_refs",
    "attach_neighborhood_overflow_ref",
    "augment_insight_with_semantic",
    "build_calls_insight",
    "build_entity_insight",
    "build_neighborhood_from_slices",
    "build_search_insight",
    "confidence_from_findings",
    "default_calls_budget",
    "default_entity_budget",
    "default_search_budget",
    "degradation_from_summary",
    "empty_neighborhood",
    "mark_partial_for_missing_languages",
    "max_bucket",
    "merge_slice",
    "node_refs_from_semantic_entries",
    "read_reference_total",
    "read_total",
    "risk_from_counters",
    "string_or_none",
    "target_from_finding",
]
