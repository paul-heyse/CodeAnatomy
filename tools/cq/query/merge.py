"""Query merge helper wrappers for auto-scope execution."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from tools.cq.core.bootstrap import resolve_runtime_services
from tools.cq.core.multilang_orchestrator import (
    merge_language_cq_results,
    runmeta_for_scope_merge,
)
from tools.cq.core.requests import MergeResultsRequest
from tools.cq.core.schema import CqResult
from tools.cq.core.services import EntityFrontDoorRequest
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.ir import Query
from tools.cq.query.language import QueryLanguage
from tools.cq.search.multilang_diagnostics import (
    build_capability_diagnostics,
    build_cross_language_diagnostics,
    build_language_capabilities,
    diagnostics_to_summary_payload,
    features_from_query,
    is_python_oriented_query_ir,
)


def _count_result_matches(result: CqResult | None) -> int:
    if result is None:
        return 0
    summary_matches = result.summary.get("matches")
    if isinstance(summary_matches, int):
        return summary_matches
    summary_total = result.summary.get("total_matches")
    if isinstance(summary_total, int):
        return summary_total
    return len(result.key_findings)


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


def _coerce_semantic_telemetry(payload: object) -> tuple[int, int, int, int]:
    if not isinstance(payload, dict):
        return 0, 0, 0, 0
    attempted = payload.get("attempted")
    applied = payload.get("applied")
    failed = payload.get("failed")
    timed_out = payload.get("timed_out")
    return (
        attempted if isinstance(attempted, int) else 0,
        applied if isinstance(applied, int) else 0,
        failed if isinstance(failed, int) else 0,
        timed_out if isinstance(timed_out, int) else 0,
    )


def _merge_semantic_contract_inputs(
    summary: dict[str, object],
) -> tuple[str, bool, int, int, int, int]:
    py_attempted, py_applied, py_failed, py_timed_out = _coerce_semantic_telemetry(
        summary.get("python_semantic_telemetry")
    )
    rust_attempted, rust_applied, rust_failed, rust_timed_out = _coerce_semantic_telemetry(
        summary.get("rust_semantic_telemetry")
    )
    provider = "none"
    if rust_attempted > 0 and py_attempted <= 0:
        provider = "rust_static"
    elif py_attempted > 0:
        provider = "python_static"
    elif rust_attempted > 0:
        provider = "rust_static"
    elif summary.get("lang_scope") == "auto":
        provider = "python_static"
    attempted = py_attempted + rust_attempted
    applied = py_applied + rust_applied
    failed = py_failed + rust_failed
    timed_out = py_timed_out + rust_timed_out
    return provider, provider != "none", attempted, applied, failed, timed_out


def _mark_entity_insight_partial_from_summary(result: CqResult) -> None:
    import msgspec

    from tools.cq.core.front_door_insight import (
        coerce_front_door_insight,
        mark_partial_for_missing_languages,
        to_public_front_door_insight_dict,
    )
    from tools.cq.search.semantic_contract_state import (
        SemanticContractStateInputV1,
        SemanticProvider,
        derive_semantic_contract_state,
    )

    insight = coerce_front_door_insight(result.summary.get("front_door_insight"))
    if insight is None:
        return
    lang_scope = result.summary.get("lang_scope")
    if isinstance(lang_scope, str) and lang_scope == "auto":
        missing = _missing_languages_from_summary(result.summary)
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
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)


def merge_auto_scope_query_results(
    *,
    query: Query,
    results: dict[QueryLanguage, CqResult],
    root: Path,
    argv: list[str],
    tc: Toolchain,
    summary_common: dict[str, object],
) -> CqResult:
    """Merge per-language query results using canonical executor behavior.

    Returns:
        Merged CQ result using canonical auto-scope semantics.
    """
    diagnostics = build_cross_language_diagnostics(
        lang_scope=query.lang_scope,
        python_matches=_count_result_matches(results.get("python")),
        rust_matches=_count_result_matches(results.get("rust")),
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
        root=root,
        argv=argv,
        tc=tc,
    )
    merged = merge_language_cq_results(
        MergeResultsRequest(
            scope=query.lang_scope,
            results=results,
            run=run,
            diagnostics=diagnostics,
            diagnostic_payloads=diagnostic_payloads,
            language_capabilities=language_capabilities,
            summary_common=summary_common,
        )
    )
    if query.is_pattern_query:
        return merged
    if "front_door_insight" in merged.summary:
        _mark_entity_insight_partial_from_summary(merged)
        return merged
    services = resolve_runtime_services(root)
    services.entity.attach_front_door(
        EntityFrontDoorRequest(result=merged),
    )
    return merged


__all__ = ["merge_auto_scope_query_results"]
