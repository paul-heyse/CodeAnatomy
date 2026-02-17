"""Search semantic state and outcome resolution logic."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.schema import Anchor, Finding
from tools.cq.core.semantic_contracts import (
    SemanticContractStateInputV1,
    SemanticContractStateV1,
    SemanticProvider,
    derive_semantic_contract_state,
)
from tools.cq.core.summary_contract import (
    CqSummary,
    SemanticTelemetryV1,
    build_semantic_telemetry,
    coerce_semantic_telemetry,
)
from tools.cq.query.language import QueryLanguage
from tools.cq.search.pipeline._semantic_helpers import (
    count_mapping_rows as _count_mapping_rows,
)
from tools.cq.search.pipeline._semantic_helpers import (
    normalize_python_semantic_degradation_reason as _normalize_python_semantic_degradation_reason,
)
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.enrichment_contracts import python_semantic_enrichment_payload
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch, _SearchSemanticOutcome
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentRequest,
    enrich_with_language_semantics,
    infer_language_for_path,
    provider_for_language,
    semantic_runtime_enabled,
)

if TYPE_CHECKING:
    from tools.cq.core.front_door_assembly import FrontDoorInsightV1


def _payload_coverage_status(payload: dict[str, object]) -> tuple[str | None, str | None]:
    """Extract coverage status and reason from payload.

    Parameters
    ----------
    payload
        Semantic enrichment payload.

    Returns:
    -------
    tuple[str | None, str | None]
        Coverage status and reason (if present).
    """
    status = payload.get("enrichment_status")
    if isinstance(status, str):
        reason = payload.get("degrade_reason")
        return status, reason if isinstance(reason, str) and reason else None
    coverage = payload.get("coverage")
    if isinstance(coverage, dict):
        cov_status = coverage.get("status")
        cov_reason = coverage.get("reason")
        return (
            cov_status if isinstance(cov_status, str) else None,
            cov_reason if isinstance(cov_reason, str) and cov_reason else None,
        )
    return None, None


def _rust_payload_has_signal(payload: dict[str, object]) -> bool:
    """Check if rust semantic payload has actionable signal.

    Parameters
    ----------
    payload
        Rust semantic enrichment payload.

    Returns:
    -------
    bool
        True if payload contains actionable signal.
    """

    def _has_rows(container: object, keys: tuple[str, ...]) -> bool:
        if not isinstance(container, dict):
            return False
        return any(_count_mapping_rows(container.get(key)) > 0 for key in keys)

    has_signal = (
        _has_rows(
            payload.get("symbol_grounding"),
            ("definitions", "type_definitions", "implementations", "references"),
        )
        or _has_rows(payload.get("call_graph"), ("incoming_callers", "outgoing_callees"))
        or _has_rows(payload.get("type_hierarchy"), ("supertypes", "subtypes"))
        or _count_mapping_rows(payload.get("document_symbols")) > 0
        or _count_mapping_rows(payload.get("diagnostics")) > 0
    )
    if has_signal:
        return True
    hover = payload.get("hover_text")
    return isinstance(hover, str) and bool(hover.strip())


def _rust_payload_reason(payload: dict[str, object]) -> str | None:
    """Extract degradation reason from rust semantic payload.

    Parameters
    ----------
    payload
        Rust semantic enrichment payload.

    Returns:
    -------
    str | None
        Degradation reason if present.
    """
    semantic_planes = payload.get("semantic_planes")
    if isinstance(semantic_planes, dict):
        degradation = semantic_planes.get("degradation")
        if isinstance(degradation, list):
            for reason in degradation:
                if isinstance(reason, str) and reason:
                    return reason
        reason = semantic_planes.get("reason")
        if isinstance(reason, str) and reason:
            return reason
    degrade_events = payload.get("degrade_events")
    if isinstance(degrade_events, list):
        for event in degrade_events:
            if not isinstance(event, dict):
                continue
            category = event.get("category")
            if isinstance(category, str) and category:
                return category
    return None


def _resolve_search_semantic_target(
    *,
    ctx: SearchConfig,
    primary_target_finding: Finding | None,
    primary_target_match: EnrichedMatch | None,
) -> tuple[Path, QueryLanguage, Anchor, str] | None:
    """Resolve semantic enrichment target from primary target.

    Parameters
    ----------
    ctx
        Search context with configuration.
    primary_target_finding
        Primary target finding (if available).
    primary_target_match
        Primary target match (if available).

    Returns:
    -------
    tuple[Path, QueryLanguage, Anchor, str] | None
        Target file path, language, anchor, and symbol hint (if resolved).
    """
    if primary_target_finding is None and primary_target_match is None:
        return None

    anchor = primary_target_finding.anchor if primary_target_finding is not None else None
    if anchor is None and primary_target_match is not None:
        anchor = Anchor.from_span(primary_target_match.span)
    if anchor is None:
        return None

    target_file_path = ctx.root / anchor.file
    inferred_language = infer_language_for_path(target_file_path)
    target_language = (
        primary_target_match.language
        if primary_target_match is not None and primary_target_match.language in {"python", "rust"}
        else inferred_language
    )
    if target_language not in {"python", "rust"}:
        return None

    name_value = primary_target_finding.details.get("name") if primary_target_finding else None
    symbol_hint = str(name_value) if isinstance(name_value, str) else None
    if not symbol_hint and primary_target_match is not None:
        symbol_hint = primary_target_match.match_text
    if not symbol_hint:
        symbol_hint = ctx.query
    return target_file_path, target_language, anchor, symbol_hint


def _apply_prefetched_search_semantic_outcome(
    *,
    outcome: _SearchSemanticOutcome,
    target_language: QueryLanguage,
    primary_target_match: EnrichedMatch | None,
) -> bool:
    """Apply prefetched python semantic outcome from primary target match.

    Parameters
    ----------
    outcome
        Semantic outcome accumulator to update.
    target_language
        Target language for enrichment.
    primary_target_match
        Primary target match (may have prefetched payload).

    Returns:
    -------
    bool
        True if prefetched payload was applied.
    """
    if target_language != "python" or primary_target_match is None:
        return False

    payload = python_semantic_enrichment_payload(primary_target_match.python_semantic_enrichment)
    if not payload:
        return False
    prefetch_status, prefetch_reason = _payload_coverage_status(payload)
    if prefetch_status in {"applied", "degraded", "partial_signal", "structural_only"}:
        outcome.payload = payload
        outcome.applied = 1
    elif payload:
        # Retain closest-fit payload even when strict signal threshold is not met.
        outcome.payload = payload
        outcome.applied = 1
        if prefetch_reason:
            outcome.reasons.append(prefetch_reason)
    else:
        outcome.failed = 1
        outcome.reasons.append(
            _normalize_python_semantic_degradation_reason(
                reasons=(),
                coverage_reason=prefetch_reason,
            )
        )
    return True


def _apply_search_semantic_payload_outcome(
    *,
    outcome: _SearchSemanticOutcome,
    target_language: QueryLanguage,
    payload: dict[str, object] | None,
    timed_out: bool,
    failure_reason: str | None,
) -> None:
    """Apply semantic payload outcome to accumulator.

    Parameters
    ----------
    outcome
        Semantic outcome accumulator to update.
    target_language
        Target language for enrichment.
    payload
        Semantic enrichment payload or None.
    timed_out
        Whether request timed out.
    failure_reason
        Failure reason if any.
    """
    outcome.payload = payload
    outcome.timed_out = int(timed_out)
    if payload is None:
        outcome.failed = 1
        normalized_reason = (
            _normalize_python_semantic_degradation_reason(
                reasons=(failure_reason,) if isinstance(failure_reason, str) else (),
                coverage_reason=failure_reason,
            )
            if target_language == "python"
            else (failure_reason or ("request_timeout" if timed_out else "request_failed"))
        )
        outcome.reasons.append(normalized_reason)
        return

    if target_language == "rust":
        if _rust_payload_has_signal(payload):
            outcome.applied = 1
            return
        outcome.failed = 1
        outcome.reasons.append(_rust_payload_reason(payload) or failure_reason or "no_signal")
        outcome.payload = None
        return

    payload_status, payload_reason = _payload_coverage_status(payload)
    if payload_status in {"applied", "degraded", "partial_signal", "structural_only"}:
        outcome.applied = 1
        return
    if payload:
        outcome.applied = 1
        if payload_reason:
            outcome.reasons.append(payload_reason)
        return

    outcome.failed = 1
    outcome.reasons.append(
        _normalize_python_semantic_degradation_reason(
            reasons=(failure_reason,) if isinstance(failure_reason, str) else (),
            coverage_reason=payload_reason or failure_reason,
        )
    )
    outcome.payload = None


def collect_search_semantic_outcome(
    *,
    ctx: SearchConfig,
    primary_target_finding: Finding | None,
    primary_target_match: EnrichedMatch | None,
) -> _SearchSemanticOutcome:
    """Collect semantic enrichment outcome for primary search target.

    Parameters
    ----------
    ctx
        Search context with configuration.
    primary_target_finding
        Primary target finding (if available).
    primary_target_match
        Primary target match (if available).

    Returns:
    -------
    _SearchSemanticOutcome
        Semantic enrichment outcome with payload and telemetry.
    """
    outcome = _SearchSemanticOutcome()
    resolved = _resolve_search_semantic_target(
        ctx=ctx,
        primary_target_finding=primary_target_finding,
        primary_target_match=primary_target_match,
    )
    if resolved is None:
        if primary_target_match is not None or primary_target_finding is not None:
            outcome.reasons.append("provider_unavailable")
        return outcome

    target_file_path, target_language, anchor, symbol_hint = resolved

    outcome.provider = provider_for_language(target_language)
    outcome.target_language = target_language
    if not semantic_runtime_enabled():
        outcome.reasons.append("not_attempted_runtime_disabled")
        return outcome

    outcome.attempted = 1
    if _apply_prefetched_search_semantic_outcome(
        outcome=outcome,
        target_language=target_language,
        primary_target_match=primary_target_match,
    ):
        return outcome

    semantic_outcome = enrich_with_language_semantics(
        LanguageSemanticEnrichmentRequest(
            language=target_language,
            mode="search",
            root=ctx.root,
            file_path=target_file_path,
            line=max(1, int(anchor.line)),
            col=int(anchor.col or 0),
            run_id=ctx.run_id,
            symbol_hint=symbol_hint,
        )
    )
    _apply_search_semantic_payload_outcome(
        outcome=outcome,
        target_language=target_language,
        payload=semantic_outcome.payload,
        timed_out=semantic_outcome.timed_out,
        failure_reason=semantic_outcome.failure_reason,
    )
    return outcome


def update_search_summary_semantic_telemetry(
    summary: CqSummary,
    outcome: _SearchSemanticOutcome,
) -> None:
    """Update search summary with semantic enrichment telemetry.

    Parameters
    ----------
    summary
        Summary dictionary to update.
    outcome
        Semantic enrichment outcome.
    """
    if outcome.attempted <= 0 or outcome.target_language not in {"python", "rust"}:
        return
    telemetry_key = (
        "python_semantic_telemetry"
        if outcome.target_language == "python"
        else "rust_semantic_telemetry"
    )
    telemetry = coerce_semantic_telemetry(getattr(summary, telemetry_key))
    if telemetry is None:
        telemetry = SemanticTelemetryV1()
    updated = build_semantic_telemetry(
        attempted=telemetry.attempted + outcome.attempted,
        applied=telemetry.applied + outcome.applied,
        failed=telemetry.failed + outcome.failed,
        skipped=telemetry.skipped,
        timed_out=telemetry.timed_out + outcome.timed_out,
    )
    if telemetry_key == "python_semantic_telemetry":
        summary.python_semantic_telemetry = updated
    else:
        summary.rust_semantic_telemetry = updated


def read_semantic_telemetry(
    summary: CqSummary,
    *,
    language: QueryLanguage | None,
) -> tuple[int, int, int, int]:
    """Read semantic telemetry counters from summary.

    Parameters
    ----------
    summary
        Summary dictionary.
    language
        Language to read telemetry for (or None for all).

    Returns:
    -------
    tuple[int, int, int, int]
        Attempted, applied, failed, and timed_out counts.
    """

    def _read_entry(raw: SemanticTelemetryV1 | None) -> tuple[int, int, int, int]:
        if raw is None:
            return 0, 0, 0, 0
        return (
            raw.attempted,
            raw.applied,
            raw.failed,
            raw.timed_out,
        )

    if language == "python":
        return _read_entry(coerce_semantic_telemetry(summary.python_semantic_telemetry))
    if language == "rust":
        return _read_entry(coerce_semantic_telemetry(summary.rust_semantic_telemetry))

    py = _read_entry(coerce_semantic_telemetry(summary.python_semantic_telemetry))
    rust = _read_entry(coerce_semantic_telemetry(summary.rust_semantic_telemetry))
    return (
        py[0] + rust[0],
        py[1] + rust[1],
        py[2] + rust[2],
        py[3] + rust[3],
    )


def derive_semantic_provider_from_summary(summary: CqSummary) -> SemanticProvider:
    """Derive semantic provider from summary telemetry.

    Parameters
    ----------
    summary
        Summary dictionary with telemetry.

    Returns:
    -------
    SemanticProvider
        Derived semantic provider identifier.
    """
    py_attempted, _py_applied, _py_failed, _py_timed_out = read_semantic_telemetry(
        summary, language="python"
    )
    rust_attempted, _rs_applied, _rs_failed, _rs_timed_out = read_semantic_telemetry(
        summary, language="rust"
    )
    if rust_attempted > 0 and py_attempted <= 0:
        return "rust_static"
    if py_attempted > 0:
        return "python_static"
    if rust_attempted > 0:
        return "rust_static"
    return "none"


def derive_search_semantic_state(
    summary: CqSummary,
    outcome: _SearchSemanticOutcome,
) -> SemanticContractStateV1:
    """Derive semantic contract state from outcome and summary.

    Parameters
    ----------
    summary
        Summary dictionary with telemetry.
    outcome
        Semantic enrichment outcome.

    Returns:
    -------
    SemanticContractStateV1
        Derived semantic contract state.
    """
    provider_value: SemanticProvider
    if outcome.provider in {"python_static", "rust_static"}:
        provider_value = outcome.provider
    elif outcome.provider == "none":
        provider_value = derive_semantic_provider_from_summary(summary)
    else:
        provider_value = "none"
    attempted, applied, failed, timed_out = read_semantic_telemetry(
        summary,
        language=cast("QueryLanguage | None", outcome.target_language),
    )
    reasons = list(outcome.reasons)
    if provider_value == "none":
        reasons.append("provider_unavailable")
    elif attempted <= 0 and "not_attempted_runtime_disabled" not in reasons:
        reasons.append("not_attempted_by_design")
    if outcome.attempted > 0 and outcome.applied <= 0 and applied > 0:
        reasons.append("top_target_failed")
    return derive_semantic_contract_state(
        SemanticContractStateInputV1(
            provider=provider_value,
            available=provider_value != "none",
            attempted=attempted,
            applied=applied,
            failed=max(failed, attempted - applied if attempted > applied else 0),
            timed_out=timed_out,
            reasons=tuple(dict.fromkeys(reasons)),
        )
    )


def apply_search_semantic_insight(
    *,
    ctx: SearchConfig,
    insight: FrontDoorInsightV1,
    summary: CqSummary,
    primary_target_finding: Finding | None,
    primary_target_match: EnrichedMatch | None,
) -> FrontDoorInsightV1:
    """Apply semantic enrichment to search insight.

    Parameters
    ----------
    ctx
        Search context with configuration.
    insight
        Front door insight to augment.
    summary
        Summary dictionary to update with semantic telemetry.
    primary_target_finding
        Primary target finding (if available).
    primary_target_match
        Primary target match (if available).

    Returns:
    -------
    FrontDoorInsightV1
        Augmented insight with semantic contract state.
    """
    from tools.cq.core.front_door_assembly import augment_insight_with_semantic

    outcome = collect_search_semantic_outcome(
        ctx=ctx,
        primary_target_finding=primary_target_finding,
        primary_target_match=primary_target_match,
    )
    if outcome.payload is not None:
        insight = augment_insight_with_semantic(insight, outcome.payload)
        semantic_planes = outcome.payload.get("semantic_planes")
        if isinstance(semantic_planes, dict):
            summary.semantic_planes = dict(semantic_planes)
    update_search_summary_semantic_telemetry(summary, outcome)
    semantic_state = derive_search_semantic_state(summary, outcome)
    return msgspec.structs.replace(
        insight,
        degradation=msgspec.structs.replace(
            insight.degradation,
            semantic=semantic_state.status,
            notes=tuple(dict.fromkeys([*insight.degradation.notes, *semantic_state.reasons])),
        ),
    )


__all__ = [
    "apply_search_semantic_insight",
    "collect_search_semantic_outcome",
    "derive_search_semantic_state",
    "derive_semantic_provider_from_summary",
    "read_semantic_telemetry",
    "update_search_summary_semantic_telemetry",
]
