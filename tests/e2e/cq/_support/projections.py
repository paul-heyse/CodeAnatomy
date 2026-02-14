# ruff: noqa: DOC201
"""Stable projection helpers for CQ E2E snapshots."""

from __future__ import annotations

import re
from typing import Any

from tools.cq.core.front_door_insight import coerce_front_door_insight
from tools.cq.core.schema import CqResult

_DURATION_PATTERN = re.compile(r"(\*\*(?:Created|Elapsed):\*\*)\s+[0-9]+(?:\.[0-9]+)?ms")


def _normalize_message(message: str) -> str:
    return _DURATION_PATTERN.sub(r"\1 <duration_ms>", message)


def _normalize_path(path: str) -> str:
    return path.removeprefix("./")


def _project_front_door_insight(summary: dict[str, object]) -> dict[str, Any] | None:
    insight = coerce_front_door_insight(summary.get("front_door_insight"))
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
            "lsp": insight.degradation.lsp,
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


def _project_step_summaries(summary: dict[str, object]) -> dict[str, Any]:
    step_summaries = summary.get("step_summaries")
    if not isinstance(step_summaries, dict):
        return {}
    projected: dict[str, Any] = {}
    for step_id, step_summary in step_summaries.items():
        if not isinstance(step_id, str) or not isinstance(step_summary, dict):
            continue
        insight = step_summary.get("front_door_insight")
        degradation: dict[str, object] = {}
        if isinstance(insight, dict):
            maybe_degradation = insight.get("degradation")
            if isinstance(maybe_degradation, dict):
                degradation = maybe_degradation
        projected[step_id] = {
            "mode": step_summary.get("mode"),
            "query": step_summary.get("query"),
            "pyrefly_telemetry": step_summary.get("pyrefly_telemetry"),
            "rust_lsp_telemetry": step_summary.get("rust_lsp_telemetry"),
            "front_door_degradation_lsp": degradation,
        }
    return projected


def result_snapshot_projection(result: CqResult) -> dict[str, Any]:
    """Project a CqResult into a stable snapshot payload.

    This intentionally excludes volatile detail payloads and timing-heavy
    enrichment telemetry while preserving top-level behavioral contracts.
    """
    summary = result.summary
    projected_step_summaries = _project_step_summaries(summary)
    projected_summary = {
        "mode": summary.get("mode"),
        "query": summary.get("query"),
        "lang_scope": summary.get("lang_scope"),
        "language_order": summary.get("language_order"),
        "steps": summary.get("steps"),
        "plan_version": summary.get("plan_version"),
        "target": summary.get("target"),
        "target_name": summary.get("target_name"),
        "target_file": summary.get("target_file"),
        "target_resolution_kind": summary.get("target_resolution_kind"),
        "top_k": summary.get("top_k"),
        "enable_lsp": summary.get("enable_lsp"),
        "total_slices": summary.get("total_slices"),
        "total_nodes": summary.get("total_nodes"),
        "total_edges": summary.get("total_edges"),
        "total_diagnostics": summary.get("total_diagnostics"),
        "pyrefly_telemetry": summary.get("pyrefly_telemetry"),
        "rust_lsp_telemetry": summary.get("rust_lsp_telemetry"),
        "lsp_advanced_planes_present": isinstance(summary.get("lsp_advanced_planes"), dict)
        and bool(summary.get("lsp_advanced_planes")),
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
