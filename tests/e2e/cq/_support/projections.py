"""Stable projection helpers for CQ E2E snapshots."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from tools.cq.core.front_door_render import coerce_front_door_insight
from tools.cq.core.schema import CqResult
from tools.cq.core.summary_types import SemanticTelemetryV1, SummaryEnvelopeV1

_DURATION_PATTERN = re.compile(r"(\*\*(?:Created|Elapsed):\*\*)\s+[0-9]+(?:\.[0-9]+)?ms")
_BUNDLE_ID_PATTERN = re.compile(r"(\*\*Bundle ID:\*\*)\s+\S+")
_RELATIONSHIPS_PATTERN = re.compile(r"^\*\*Relationships:\*\*\s+(.+)$")


def _normalize_message(message: str) -> str:
    normalized = _DURATION_PATTERN.sub(r"\1 <duration_ms>", message)
    normalized = _BUNDLE_ID_PATTERN.sub(r"\1 <bundle_id>", normalized)
    relationships_match = _RELATIONSHIPS_PATTERN.match(normalized)
    if relationships_match is None:
        return normalized
    values = [part.strip() for part in relationships_match.group(1).split(",") if part.strip()]
    values.sort()
    return f"**Relationships:** {', '.join(values)}"


def _normalize_path(path: str) -> str:
    return path.removeprefix("./")


def _summary_mapping(summary: SummaryEnvelopeV1 | Mapping[str, object]) -> Mapping[str, object]:
    if isinstance(summary, SummaryEnvelopeV1):
        return summary.to_dict()
    return summary


def _project_semantic_telemetry(value: object) -> dict[str, int] | None:
    if isinstance(value, SemanticTelemetryV1):
        return {
            "attempted": value.attempted,
            "applied": value.applied,
            "failed": value.failed,
            "skipped": value.skipped,
            "timed_out": value.timed_out,
        }
    if isinstance(value, Mapping):
        return {
            "attempted": int(value.get("attempted", 0) or 0),
            "applied": int(value.get("applied", 0) or 0),
            "failed": int(value.get("failed", 0) or 0),
            "skipped": int(value.get("skipped", 0) or 0),
            "timed_out": int(value.get("timed_out", 0) or 0),
        }
    return None


def _int_or_fallback(value: object, fallback: int) -> int:
    if isinstance(value, bool):
        return fallback
    if isinstance(value, int):
        return value
    return fallback


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _project_front_door_insight(
    summary: SummaryEnvelopeV1 | Mapping[str, object],
) -> dict[str, Any] | None:
    summary_map = _summary_mapping(summary)
    insight = coerce_front_door_insight(summary_map.get("front_door_insight"))
    if insight is None:
        return None
    return {
        "source": insight.source,
        "target": {
            "symbol": insight.target.symbol,
            "kind": insight.target.kind,
            "selection_reason": insight.target.selection_reason,
            "location": {
                "file": _normalize_path(insight.target.location.file),
                "line": insight.target.location.line,
                "col": insight.target.location.col,
            },
        },
        "neighborhood": {
            "callers": {
                "total": insight.neighborhood.callers.total,
                "availability": insight.neighborhood.callers.availability,
            },
            "callees": {
                "total": insight.neighborhood.callees.total,
                "availability": insight.neighborhood.callees.availability,
            },
            "references": {
                "total": insight.neighborhood.references.total,
                "availability": insight.neighborhood.references.availability,
            },
            "hierarchy_or_scope": {
                "total": insight.neighborhood.hierarchy_or_scope.total,
                "availability": insight.neighborhood.hierarchy_or_scope.availability,
            },
        },
        "risk": {
            "level": insight.risk.level,
            "counters": {
                "callers": insight.risk.counters.callers,
                "callees": insight.risk.counters.callees,
                "hazard_count": insight.risk.counters.hazard_count,
                "forwarding_count": insight.risk.counters.forwarding_count,
            },
        },
        "degradation": {
            "semantic": insight.degradation.semantic,
            "scan": insight.degradation.scan,
            "scope_filter": insight.degradation.scope_filter,
            "notes": list(insight.degradation.notes),
        },
        "artifact_ref_presence": {
            "diagnostics": bool(insight.artifact_refs.diagnostics),
            "telemetry": bool(insight.artifact_refs.telemetry),
            "neighborhood_overflow": bool(insight.artifact_refs.neighborhood_overflow),
        },
    }


def _project_step_summaries(summary: SummaryEnvelopeV1 | Mapping[str, object]) -> dict[str, Any]:
    step_summaries = _summary_mapping(summary).get("step_summaries")
    if not isinstance(step_summaries, Mapping):
        return {}
    projected: dict[str, Any] = {}
    for step_id, step_summary in step_summaries.items():
        if not isinstance(step_id, str) or not isinstance(step_summary, Mapping):
            continue
        insight = step_summary.get("front_door_insight")
        degradation: dict[str, object] = {}
        if isinstance(insight, Mapping):
            maybe_degradation = insight.get("degradation")
            if isinstance(maybe_degradation, Mapping):
                degradation = dict(maybe_degradation)
        projected[step_id] = {
            "mode": step_summary.get("mode"),
            "query": step_summary.get("query"),
            "python_semantic_telemetry": _project_semantic_telemetry(
                step_summary.get("python_semantic_telemetry")
            ),
            "rust_semantic_telemetry": _project_semantic_telemetry(
                step_summary.get("rust_semantic_telemetry")
            ),
            "front_door_degradation_semantic": degradation,
        }
    return projected


def result_snapshot_projection(result: CqResult) -> dict[str, Any]:
    """Project a CqResult into a stable snapshot payload.

    This intentionally excludes volatile detail payloads and timing-heavy
    enrichment telemetry while preserving top-level behavioral contracts.

    Returns:
        dict[str, Any]: Stable projection for snapshot assertions.
    """
    summary = result.summary
    summary_map = _summary_mapping(summary)
    projected_step_summaries = _project_step_summaries(summary)
    include_neighborhood_totals = result.run.macro == "neighborhood"
    projected_summary = {
        "mode": summary_map.get("mode"),
        "query": summary_map.get("query"),
        "lang_scope": summary_map.get("lang_scope"),
        "language_order": summary_map.get("language_order"),
        "steps": summary_map.get("steps"),
        "plan_version": summary_map.get("plan_version"),
        "target": summary_map.get("target"),
        "target_name": summary_map.get("target_name"),
        "target_file": summary_map.get("target_file"),
        "target_resolution_kind": summary_map.get("target_resolution_kind"),
        "top_k": summary_map.get("top_k"),
        "enable_semantic_enrichment": summary_map.get("enable_semantic_enrichment"),
        "total_slices": (
            _int_or_fallback(summary_map.get("total_slices"), 0)
            if include_neighborhood_totals
            else _int_or_none(summary_map.get("total_slices"))
        ),
        "total_nodes": (
            _int_or_fallback(summary_map.get("total_nodes"), 0)
            if include_neighborhood_totals
            else _int_or_none(summary_map.get("total_nodes"))
        ),
        "total_edges": (
            _int_or_fallback(summary_map.get("total_edges"), 0)
            if include_neighborhood_totals
            else _int_or_none(summary_map.get("total_edges"))
        ),
        "total_diagnostics": (
            _int_or_fallback(summary_map.get("total_diagnostics"), 0)
            if include_neighborhood_totals
            else _int_or_none(summary_map.get("total_diagnostics"))
        ),
        "python_semantic_telemetry": _project_semantic_telemetry(
            summary_map.get("python_semantic_telemetry")
        ),
        "rust_semantic_telemetry": _project_semantic_telemetry(
            summary_map.get("rust_semantic_telemetry")
        ),
        "semantic_planes_present": isinstance(summary_map.get("semantic_planes"), dict)
        and bool(summary_map.get("semantic_planes")),
        "front_door_insight": _project_front_door_insight(summary),
    }
    if projected_step_summaries:
        projected_summary["step_summaries"] = projected_step_summaries

    key_findings = [_normalize_message(finding.message) for finding in result.key_findings[:20]]
    section_shapes = [
        {
            "title": section.title,
            "count": len(section.findings),
            "messages": [_normalize_message(finding.message) for finding in section.findings[:8]],
        }
        for section in result.sections
    ]
    anchors = sorted(
        {
            finding.anchor.file
            for finding in (*result.key_findings, *result.evidence)
            if finding.anchor is not None
        }
    )
    return {
        "summary": projected_summary,
        "key_findings": key_findings,
        "sections": section_shapes,
        "anchors": anchors,
    }
