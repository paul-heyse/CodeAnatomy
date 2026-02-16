"""Python semantic enrichment prefetch and attachment logic."""

from __future__ import annotations

from typing import cast

import msgspec

from tools.cq.core.contract_codec import to_public_dict, to_public_list
from tools.cq.search._shared.search_contracts import (
    coerce_python_semantic_diagnostics,
    coerce_python_semantic_overview,
    coerce_python_semantic_telemetry,
)
from tools.cq.search.pipeline.contracts import SmartSearchContext
from tools.cq.search.pipeline.smart_search_telemetry import (
    new_python_semantic_telemetry as _new_python_semantic_telemetry,
)
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    RawMatch,
    _LanguageSearchResult,
    _PythonSemanticAnchorKey,
    _PythonSemanticOverviewAccumulator,
    _PythonSemanticPrefetchResult,
)
from tools.cq.search.python.evidence import evaluate_python_semantic_signal_from_mapping
from tools.cq.search.semantic.models import (
    LanguageSemanticEnrichmentOutcome,
    LanguageSemanticEnrichmentRequest,
    enrich_with_language_semantics,
    semantic_runtime_enabled,
)

# Budget cap for python semantic enrichment
MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS = 8

# Non-fatal exceptions during python semantic prefetch
_PYTHON_SEMANTIC_PREFETCH_NON_FATAL_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    TypeError,
    ValueError,
)


def _python_semantic_anchor_key(
    *,
    file: str,
    line: int,
    col: int,
    match_text: str,
) -> _PythonSemanticAnchorKey:
    """Build anchor key for python semantic cache lookups.

    Parameters
    ----------
    file
        File path string.
    line
        Line number.
    col
        Column number.
    match_text
        Matched text string.

    Returns:
    -------
    _PythonSemanticAnchorKey
        Tuple key for anchor-based lookups.
    """
    return (file, line, col, match_text)


def _python_semantic_anchor_key_from_raw(raw: RawMatch) -> _PythonSemanticAnchorKey:
    """Build anchor key from raw match.

    Parameters
    ----------
    raw
        Raw match from candidate collection.

    Returns:
    -------
    _PythonSemanticAnchorKey
        Tuple key for anchor-based lookups.
    """
    return _python_semantic_anchor_key(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        match_text=raw.match_text,
    )


def _python_semantic_anchor_key_from_match(match: EnrichedMatch) -> _PythonSemanticAnchorKey:
    """Build anchor key from enriched match.

    Parameters
    ----------
    match
        Enriched match from classification.

    Returns:
    -------
    _PythonSemanticAnchorKey
        Tuple key for anchor-based lookups.
    """
    return _python_semantic_anchor_key(
        file=match.file,
        line=match.line,
        col=match.col,
        match_text=match.match_text,
    )


def _python_semantic_failure_diagnostic(
    *,
    reason: str = "session_unavailable",
) -> dict[str, object]:
    """Create diagnostic payload for python semantic failure.

    Parameters
    ----------
    reason
        Failure reason string.

    Returns:
    -------
    dict[str, object]
        Diagnostic payload dictionary.
    """
    return {
        "code": "PYTHON_SEMANTIC001",
        "severity": "warning",
        "message": "PythonSemantic enrichment unavailable for one or more anchors",
        "reason": reason,
    }


def _python_semantic_no_signal_diagnostic(
    reasons: tuple[str, ...],
    *,
    coverage_reason: str | None = None,
) -> dict[str, object]:
    """Create diagnostic payload for no-signal case.

    Parameters
    ----------
    reasons
        Collection of degradation reasons.
    coverage_reason
        Optional coverage-specific reason.

    Returns:
    -------
    dict[str, object]
        Diagnostic payload dictionary.
    """
    reason_text = _normalize_python_semantic_degradation_reason(
        reasons=reasons,
        coverage_reason=coverage_reason,
    )
    return {
        "code": "PYTHON_SEMANTIC002",
        "severity": "info",
        "message": "PythonSemantic payload resolved but contained no actionable signal",
        "reason": reason_text,
    }


def _python_semantic_partial_signal_diagnostic(
    reasons: tuple[str, ...],
    *,
    coverage_reason: str | None = None,
) -> dict[str, object]:
    """Create diagnostic payload for partial signal case.

    Parameters
    ----------
    reasons
        Collection of degradation reasons.
    coverage_reason
        Optional coverage-specific reason.

    Returns:
    -------
    dict[str, object]
        Diagnostic payload dictionary.
    """
    reason_text = _normalize_python_semantic_degradation_reason(
        reasons=reasons,
        coverage_reason=coverage_reason,
    )
    return {
        "code": "PYTHON_SEMANTIC003",
        "severity": "info",
        "message": "PythonSemantic payload retained with partial signal",
        "reason": reason_text,
    }


def _normalize_python_semantic_degradation_reason(
    *,
    reasons: tuple[str, ...],
    coverage_reason: str | None,
) -> str:
    """Normalize degradation reason from multiple sources.

    Parameters
    ----------
    reasons
        Collection of degradation reasons.
    coverage_reason
        Optional coverage-specific reason.

    Returns:
    -------
    str
        Normalized reason string.
    """
    explicit_reasons = {
        "unsupported_capability",
        "capability_probe_unavailable",
        "request_interface_unavailable",
        "request_failed",
        "request_timeout",
        "session_unavailable",
        "timeout",
        "no_signal",
    }
    normalized_coverage_reason = coverage_reason
    if normalized_coverage_reason and normalized_coverage_reason.startswith(
        "no_python_semantic_signal:"
    ):
        normalized_coverage_reason = normalized_coverage_reason.removeprefix(
            "no_python_semantic_signal:"
        )
    if normalized_coverage_reason == "timeout":
        normalized_coverage_reason = "request_timeout"
    if (
        isinstance(normalized_coverage_reason, str)
        and normalized_coverage_reason in explicit_reasons
    ):
        return normalized_coverage_reason
    if coverage_reason:
        return "no_signal"

    for reason in reasons:
        normalized_reason = "request_timeout" if reason == "timeout" else reason
        if normalized_reason in explicit_reasons and normalized_reason != "no_signal":
            return normalized_reason
    return "no_signal"


def _python_semantic_coverage_reason(payload: dict[str, object]) -> str | None:
    """Extract coverage degradation reason from payload.

    Parameters
    ----------
    payload
        Python semantic enrichment payload.

    Returns:
    -------
    str | None
        Coverage reason if present, otherwise None.
    """
    coverage = payload.get("coverage")
    if not isinstance(coverage, dict):
        return None
    reason = coverage.get("reason")
    if isinstance(reason, str) and reason:
        return reason
    return None


def _prefetch_python_semantic_for_raw_matches(
    ctx: SmartSearchContext,
    *,
    lang: str,
    raw_matches: list[RawMatch],
) -> _PythonSemanticPrefetchResult:
    """Prefetch python semantic enrichment for raw matches.

    Parameters
    ----------
    ctx
        Search context with configuration.
    lang
        Language partition identifier.
    raw_matches
        Raw matches from candidate collection.

    Returns:
    -------
    _PythonSemanticPrefetchResult
        Prefetch result with payloads and telemetry.
    """
    if lang != "python" or not raw_matches or not semantic_runtime_enabled():
        return _PythonSemanticPrefetchResult(telemetry=_new_python_semantic_telemetry())

    telemetry = _new_python_semantic_telemetry()
    payloads: dict[_PythonSemanticAnchorKey, dict[str, object]] = {}
    attempted_keys: set[_PythonSemanticAnchorKey] = set()
    diagnostics: list[dict[str, object]] = []
    python_semantic_budget_used = 0

    for raw in raw_matches:
        if python_semantic_budget_used >= MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS:
            telemetry["skipped"] += 1
            continue
        # Scope check using string comparison since lang is concrete partition
        if lang != "python":
            continue
        key = _python_semantic_anchor_key_from_raw(raw)
        if key in attempted_keys:
            continue
        attempted_keys.add(key)
        python_semantic_budget_used += 1
        telemetry["attempted"] += 1
        file_path = ctx.root / raw.file
        try:
            outcome = enrich_with_language_semantics(
                LanguageSemanticEnrichmentRequest(
                    language="python",
                    mode="search",
                    root=ctx.root,
                    file_path=file_path,
                    line=raw.line,
                    col=raw.col,
                    run_id=ctx.run_id,
                    symbol_hint=raw.match_text,
                )
            )
        except _PYTHON_SEMANTIC_PREFETCH_NON_FATAL_EXCEPTIONS:
            telemetry["failed"] += 1
            diagnostics.append(_python_semantic_failure_diagnostic(reason="request_failed"))
            continue
        payload = outcome.payload if isinstance(outcome.payload, dict) else None
        if outcome.timed_out:
            telemetry["timed_out"] += 1

        if isinstance(payload, dict) and payload:
            has_signal, reasons = evaluate_python_semantic_signal_from_mapping(payload)
            if has_signal:
                telemetry["applied"] += 1
                payloads[key] = payload
            else:
                telemetry["applied"] += 1
                payloads[key] = payload
                diagnostics.append(
                    _python_semantic_partial_signal_diagnostic(
                        reasons,
                        coverage_reason=_python_semantic_coverage_reason(payload),
                    )
                )
        else:
            telemetry["failed"] += 1
            diagnostics.append(
                _python_semantic_failure_diagnostic(
                    reason=_normalize_python_semantic_degradation_reason(
                        reasons=(outcome.failure_reason,)
                        if isinstance(outcome.failure_reason, str)
                        else (),
                        coverage_reason=outcome.failure_reason,
                    )
                )
            )

    return _PythonSemanticPrefetchResult(
        payloads=payloads,
        attempted_keys=attempted_keys,
        telemetry=telemetry,
        diagnostics=diagnostics,
    )


def run_prefetch_python_semantic_for_raw_matches(
    ctx: SmartSearchContext,
    *,
    lang: str,
    raw_matches: list[RawMatch],
) -> _PythonSemanticPrefetchResult:
    """Public wrapper around python semantic prefetch execution.

    Parameters
    ----------
    ctx
        Search context with configuration.
    lang
        Language partition identifier.
    raw_matches
        Raw matches from candidate collection.

    Returns:
    -------
    _PythonSemanticPrefetchResult
        Prefetch payloads and telemetry.
    """
    return _prefetch_python_semantic_for_raw_matches(ctx, lang=lang, raw_matches=raw_matches)


def _python_semantic_enrich_match(
    ctx: SmartSearchContext,
    match: EnrichedMatch,
) -> LanguageSemanticEnrichmentOutcome:
    """Enrich a single match with python semantic payload.

    Parameters
    ----------
    ctx
        Search context with configuration.
    match
        Enriched match to enrich further.

    Returns:
    -------
    LanguageSemanticEnrichmentOutcome
        Semantic enrichment outcome.
    """
    if match.language != "python":
        return LanguageSemanticEnrichmentOutcome(failure_reason="provider_unavailable")
    if not semantic_runtime_enabled():
        return LanguageSemanticEnrichmentOutcome(failure_reason="not_attempted_runtime_disabled")
    file_path = ctx.root / match.file
    if file_path.suffix not in {".py", ".pyi"}:
        return LanguageSemanticEnrichmentOutcome(failure_reason="provider_unavailable")
    return enrich_with_language_semantics(
        LanguageSemanticEnrichmentRequest(
            language="python",
            mode="search",
            root=ctx.root,
            file_path=file_path,
            line=match.line,
            col=match.col,
            run_id=ctx.run_id,
            symbol_hint=match.match_text,
        )
    )


def _seed_python_semantic_state(
    prefetched: _PythonSemanticPrefetchResult | None,
) -> tuple[dict[str, int], list[dict[str, object]]]:
    """Initialize python semantic state from prefetch result.

    Parameters
    ----------
    prefetched
        Prefetch result or None.

    Returns:
    -------
    tuple[dict[str, int], list[dict[str, object]]]
        Seeded telemetry and diagnostics.
    """
    telemetry = _new_python_semantic_telemetry()
    diagnostics: list[dict[str, object]] = []
    if prefetched is None:
        return telemetry, diagnostics
    diagnostics.extend(prefetched.diagnostics)
    for key, value in prefetched.telemetry.items():
        if key in telemetry:
            telemetry[key] += int(value)
    return telemetry, diagnostics


def _python_semantic_payload_from_prefetch(
    prefetched: _PythonSemanticPrefetchResult | None,
    key: _PythonSemanticAnchorKey,
) -> dict[str, object] | None:
    """Retrieve python semantic payload from prefetch cache.

    Parameters
    ----------
    prefetched
        Prefetch result or None.
    key
        Anchor key to look up.

    Returns:
    -------
    dict[str, object] | None
        Cached payload if present, otherwise None.
    """
    if prefetched is None or key not in prefetched.attempted_keys:
        return None
    return prefetched.payloads.get(key)


def _fetch_python_semantic_payload(
    *,
    ctx: SmartSearchContext,
    match: EnrichedMatch,
    prefetched: _PythonSemanticPrefetchResult | None,
    telemetry: dict[str, int],
) -> tuple[dict[str, object] | None, bool, str | None]:
    """Fetch python semantic payload for a match, using prefetch cache or fresh request.

    Parameters
    ----------
    ctx
        Search context with configuration.
    match
        Match to enrich.
    prefetched
        Prefetch result cache.
    telemetry
        Telemetry counters to update.

    Returns:
    -------
    tuple[dict[str, object] | None, bool, str | None]
        Payload, attempted_in_place flag, and failure reason.
    """
    if not semantic_runtime_enabled():
        return None, False, None
    key = _python_semantic_anchor_key_from_match(match)
    prefetched_payload = _python_semantic_payload_from_prefetch(prefetched, key)
    if prefetched is not None and key in prefetched.attempted_keys:
        return prefetched_payload, False, None

    telemetry["attempted"] += 1
    try:
        outcome = _python_semantic_enrich_match(ctx, match)
    except _PYTHON_SEMANTIC_PREFETCH_NON_FATAL_EXCEPTIONS:
        return None, True, "request_failed"
    if outcome.timed_out:
        telemetry["timed_out"] += 1
    return outcome.payload, True, outcome.failure_reason


def _merge_match_with_python_semantic_payload(
    *,
    match: EnrichedMatch,
    payload: dict[str, object] | None,
    attempted_in_place: bool,
    failure_reason: str | None,
    telemetry: dict[str, int],
    diagnostics: list[dict[str, object]],
) -> EnrichedMatch:
    """Merge python semantic payload into enriched match.

    Parameters
    ----------
    match
        Match to merge payload into.
    payload
        Python semantic payload or None.
    attempted_in_place
        Whether this was a fresh attempt (not from cache).
    failure_reason
        Failure reason if any.
    telemetry
        Telemetry counters to update.
    diagnostics
        Diagnostics list to append to.

    Returns:
    -------
    EnrichedMatch
        Match with merged python_semantic_enrichment field.
    """
    if isinstance(payload, dict) and payload:
        has_signal, reasons = evaluate_python_semantic_signal_from_mapping(payload)
        if not has_signal:
            if attempted_in_place:
                telemetry["applied"] += 1
                diagnostics.append(
                    _python_semantic_partial_signal_diagnostic(
                        reasons,
                        coverage_reason=_python_semantic_coverage_reason(payload),
                    )
                )
            return msgspec.structs.replace(match, python_semantic_enrichment=payload)
        if attempted_in_place:
            telemetry["applied"] += 1
        return msgspec.structs.replace(match, python_semantic_enrichment=payload)

    if attempted_in_place:
        telemetry["failed"] += 1
        diagnostics.append(
            _python_semantic_failure_diagnostic(
                reason=_normalize_python_semantic_degradation_reason(
                    reasons=(failure_reason,) if isinstance(failure_reason, str) else (),
                    coverage_reason=failure_reason,
                ),
            )
        )
    return match


def _python_semantic_summary_payload(
    *,
    overview: dict[str, object],
    telemetry: dict[str, int],
    diagnostics: list[dict[str, object]],
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    """Build python semantic summary payload.

    Parameters
    ----------
    overview
        Overview accumulator dictionary.
    telemetry
        Telemetry counters.
    diagnostics
        Diagnostics list.

    Returns:
    -------
    tuple[dict[str, object], dict[str, object], list[dict[str, object]]]
        Coerced overview, telemetry, and diagnostics.
    """
    telemetry_payload = coerce_python_semantic_telemetry(telemetry)
    diagnostics_payload = coerce_python_semantic_diagnostics(diagnostics)
    return (
        to_public_dict(coerce_python_semantic_overview(overview)),
        to_public_dict(telemetry_payload),
        to_public_list(diagnostics_payload),
    )


def _first_string(*values: object) -> str | None:
    """Extract first non-empty string from variadic arguments.

    Parameters
    ----------
    values
        Variable number of potential string values.

    Returns:
    -------
    str | None
        First non-empty string or None.
    """
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _count_mapping_rows(value: object) -> int:
    """Count dictionary rows in a list.

    Parameters
    ----------
    value
        Value to count rows in.

    Returns:
    -------
    int
        Number of dictionary rows.
    """
    if not isinstance(value, list):
        return 0
    return sum(1 for row in value if isinstance(row, dict))


def _accumulate_python_semantic_overview(
    *,
    acc: _PythonSemanticOverviewAccumulator,
    payload: dict[str, object],
    match_text: str,
) -> None:
    """Accumulate python semantic overview statistics from payload.

    Parameters
    ----------
    acc
        Overview accumulator to update.
    payload
        Python semantic enrichment payload.
    match_text
        Match text for symbol fallback.
    """
    acc.enriched_count += 1
    type_contract = payload.get("type_contract")
    if isinstance(type_contract, dict):
        acc.primary_symbol = _first_string(
            acc.primary_symbol,
            type_contract.get("callable_signature"),
            type_contract.get("resolved_type"),
            match_text,
        )
    class_ctx = payload.get("class_method_context")
    if isinstance(class_ctx, dict):
        acc.enclosing_class = _first_string(acc.enclosing_class, class_ctx.get("enclosing_class"))
    call_graph = payload.get("call_graph")
    if isinstance(call_graph, dict):
        incoming = call_graph.get("incoming_total")
        outgoing = call_graph.get("outgoing_total")
        if isinstance(incoming, int):
            acc.total_incoming += incoming
        if isinstance(outgoing, int):
            acc.total_outgoing += outgoing
    grounding = payload.get("symbol_grounding")
    if isinstance(grounding, dict):
        acc.total_implementations += _count_mapping_rows(grounding.get("implementation_targets"))
    acc.diagnostics += _count_mapping_rows(payload.get("anchor_diagnostics"))


def _build_python_semantic_overview(matches: list[EnrichedMatch]) -> dict[str, object]:
    """Build python semantic overview from enriched matches.

    Parameters
    ----------
    matches
        Enriched matches with python semantic enrichment.

    Returns:
    -------
    dict[str, object]
        Overview summary dictionary.
    """
    acc = _PythonSemanticOverviewAccumulator()
    for match in matches:
        payload = match.python_semantic_enrichment
        if isinstance(payload, dict):
            _accumulate_python_semantic_overview(
                acc=acc, payload=payload, match_text=match.match_text
            )
    return {
        "primary_symbol": acc.primary_symbol,
        "enclosing_class": acc.enclosing_class,
        "total_incoming_callers": acc.total_incoming,
        "total_outgoing_callees": acc.total_outgoing,
        "total_implementations": acc.total_implementations,
        "targeted_diagnostics": acc.diagnostics,
        "matches_enriched": acc.enriched_count,
    }


def attach_python_semantic_enrichment(
    *,
    ctx: SmartSearchContext,
    matches: list[EnrichedMatch],
    prefetched: _PythonSemanticPrefetchResult | None = None,
) -> tuple[list[EnrichedMatch], dict[str, object], dict[str, object], list[dict[str, object]]]:
    """Attach python semantic enrichment to matches.

    Parameters
    ----------
    ctx
        Search context with configuration.
    matches
        Enriched matches to attach python semantic payloads to.
    prefetched
        Optional prefetch result for cache lookups.

    Returns:
    -------
    tuple[list[EnrichedMatch], dict[str, object], dict[str, object], list[dict[str, object]]]
        Enriched matches, overview, telemetry, and diagnostics.
    """
    if not semantic_runtime_enabled():
        return matches, {}, cast("dict[str, object]", _new_python_semantic_telemetry()), []

    telemetry, diagnostics = _seed_python_semantic_state(prefetched)

    if not matches:
        overview, telemetry_map, diagnostic_rows = _python_semantic_summary_payload(
            overview={},
            telemetry=telemetry,
            diagnostics=diagnostics,
        )
        return matches, overview, telemetry_map, diagnostic_rows

    enriched: list[EnrichedMatch] = []
    python_semantic_budget_used = 0

    for match in matches:
        if match.language != "python":
            enriched.append(match)
            continue
        if python_semantic_budget_used >= MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS:
            telemetry["skipped"] += 1
            enriched.append(match)
            continue
        python_semantic_budget_used += 1
        payload, attempted_in_place, failure_reason = _fetch_python_semantic_payload(
            ctx=ctx,
            match=match,
            prefetched=prefetched,
            telemetry=telemetry,
        )
        enriched.append(
            _merge_match_with_python_semantic_payload(
                match=match,
                payload=payload,
                attempted_in_place=attempted_in_place,
                failure_reason=failure_reason,
                telemetry=telemetry,
                diagnostics=diagnostics,
            )
        )

    overview = _build_python_semantic_overview(enriched)
    overview_map, telemetry_map, diagnostic_rows = _python_semantic_summary_payload(
        overview=overview,
        telemetry=telemetry,
        diagnostics=diagnostics,
    )
    return enriched, overview_map, telemetry_map, diagnostic_rows


def merge_matches_and_python_semantic(
    ctx: SmartSearchContext,
    partition_results: list[_LanguageSearchResult],
) -> tuple[
    list[EnrichedMatch],
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
]:
    """Merge language partition matches and attach python semantic enrichment.

    Parameters
    ----------
    ctx
        Search context with configuration.
    partition_results
        Language partition results.

    Returns:
    -------
    tuple[list[EnrichedMatch], dict[str, object], dict[str, object], list[dict[str, object]]]
        Merged enriched matches, python semantic overview, telemetry, and diagnostics.
    """
    from tools.cq.search.pipeline.smart_search import (
        _merge_language_matches,
        _merge_python_semantic_prefetch_results,
    )

    enriched_matches = _merge_language_matches(
        partition_results=partition_results,
        lang_scope=ctx.lang_scope,
    )
    prefetched_python_semantic = _merge_python_semantic_prefetch_results(partition_results)
    return attach_python_semantic_enrichment(
        ctx=ctx,
        matches=enriched_matches,
        prefetched=prefetched_python_semantic,
    )


__all__ = [
    "attach_python_semantic_enrichment",
    "merge_matches_and_python_semantic",
    "run_prefetch_python_semantic_for_raw_matches",
]
