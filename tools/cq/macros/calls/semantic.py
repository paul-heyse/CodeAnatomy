"""Semantic enrichment for call analysis.

Integrates LSP-based semantic analysis with call site results,
including hover information, diagnostics, and language-specific
semantic planes.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.macros.constants import FRONT_DOOR_PREVIEW_PER_SLICE

if TYPE_CHECKING:
    from tools.cq.core.front_door_contracts import FrontDoorInsightV1
    from tools.cq.core.schema import CqResult
    from tools.cq.core.types import QueryLanguage


@dataclass(frozen=True)
class CallsSemanticRequest:
    """Request envelope for calls-mode semantic overlay application."""

    root: Path
    target_file_path: Path | None
    target_line: int
    target_language: QueryLanguage | None
    symbol_hint: str
    preview_per_slice: int = FRONT_DOOR_PREVIEW_PER_SLICE
    run_id: str | None = None


def _apply_calls_semantic(
    *,
    insight: FrontDoorInsightV1,
    result: CqResult,
    request: CallsSemanticRequest,
) -> tuple[FrontDoorInsightV1, QueryLanguage | None, int, int, int, int, tuple[str, ...]]:
    from tools.cq.core.front_door_entity import augment_insight_with_semantic
    from tools.cq.search.semantic.models import (
        LanguageSemanticEnrichmentRequest,
        enrich_with_language_semantics,
        provider_for_language,
        semantic_runtime_enabled,
    )

    target_language = request.target_language
    semantic_attempted = 0
    semantic_applied = 0
    semantic_failed = 0
    semantic_timed_out = 0
    semantic_provider = provider_for_language(target_language) if target_language else "none"
    semantic_reasons: list[str] = []

    if request.target_file_path is not None and target_language in {"python", "rust"}:
        if semantic_runtime_enabled():
            semantic_outcome = enrich_with_language_semantics(
                LanguageSemanticEnrichmentRequest(
                    language=target_language,
                    mode="calls",
                    root=request.root,
                    file_path=request.target_file_path,
                    line=max(1, request.target_line),
                    col=0,
                    run_id=request.run_id,
                    symbol_hint=request.symbol_hint,
                )
            )
            semantic_attempted = 1
            semantic_timed_out = int(semantic_outcome.timed_out)
            semantic_payload = semantic_outcome.payload
            if semantic_payload is not None:
                payload_has_signal = _calls_payload_has_signal(target_language, semantic_payload)
                if payload_has_signal:
                    semantic_applied = 1
                    insight = augment_insight_with_semantic(
                        insight,
                        semantic_payload,
                        preview_per_slice=request.preview_per_slice,
                    )
                else:
                    semantic_failed = 1
                    semantic_reasons.append(
                        _calls_payload_reason(
                            target_language,
                            semantic_payload,
                            fallback_reason=semantic_outcome.failure_reason,
                        )
                    )
                semantic_planes = semantic_payload.get("semantic_planes")
                if isinstance(semantic_planes, dict):
                    existing_planes = result.summary.semantic_planes
                    if isinstance(existing_planes, dict):
                        existing_planes.clear()
                        existing_planes.update(dict(semantic_planes))
            else:
                semantic_failed = 1
                semantic_reasons.append(
                    semantic_outcome.failure_reason
                    or ("request_timeout" if semantic_outcome.timed_out else "request_failed")
                )
        else:
            semantic_reasons.append("not_attempted_runtime_disabled")
    elif semantic_provider == "none":
        semantic_reasons.append("provider_unavailable")
    return (
        insight,
        target_language,
        semantic_attempted,
        semantic_applied,
        semantic_failed,
        semantic_timed_out,
        tuple(dict.fromkeys(semantic_reasons)),
    )


def _calls_payload_has_signal(
    language: QueryLanguage,
    payload: dict[str, object],
) -> bool:
    if language == "python":
        coverage = payload.get("coverage")
        if isinstance(coverage, dict):
            status = coverage.get("status")
            if isinstance(status, str) and status == "applied":
                return True
        return False

    for key in ("symbol_grounding", "call_graph", "type_hierarchy"):
        value = payload.get(key)
        if isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, list) and any(isinstance(item, dict) for item in nested):
                    return True
    diagnostics = payload.get("diagnostics")
    if isinstance(diagnostics, list) and any(isinstance(item, dict) for item in diagnostics):
        return True
    hover = payload.get("hover_text")
    return isinstance(hover, str) and bool(hover.strip())


def _normalize_python_semantic_calls_reason(raw_reason: str | None) -> str:
    if not raw_reason:
        return "no_signal"
    if raw_reason == "timeout":
        return "request_timeout"
    if raw_reason.startswith("no_python_semantic_signal:"):
        normalized = raw_reason.removeprefix("no_python_semantic_signal:")
        return "request_timeout" if normalized == "timeout" else (normalized or "no_signal")
    return raw_reason


def _extract_rust_calls_reason(payload: dict[str, object]) -> str | None:
    planes = payload.get("semantic_planes")
    if isinstance(planes, dict):
        reason = planes.get("reason")
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


def _calls_payload_reason(
    language: QueryLanguage,
    payload: dict[str, object],
    *,
    fallback_reason: str | None = None,
) -> str:
    if language == "python":
        coverage = payload.get("coverage")
        if isinstance(coverage, dict):
            reason = coverage.get("reason")
            if isinstance(reason, str) and reason:
                return _normalize_python_semantic_calls_reason(reason)
        return _normalize_python_semantic_calls_reason(fallback_reason)
    return _extract_rust_calls_reason(payload) or fallback_reason or "no_signal"


__all__ = [
    "CallsSemanticRequest",
]
