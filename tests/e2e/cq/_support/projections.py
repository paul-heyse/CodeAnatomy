# ruff: noqa: DOC201
"""Stable projection helpers for CQ E2E snapshots."""

from __future__ import annotations

import re
from typing import Any

from tools.cq.core.schema import CqResult

_DURATION_PATTERN = re.compile(r"(\*\*(?:Created|Elapsed):\*\*)\s+[0-9]+(?:\.[0-9]+)?ms")


def _normalize_message(message: str) -> str:
    return _DURATION_PATTERN.sub(r"\1 <duration_ms>", message)


def result_snapshot_projection(result: CqResult) -> dict[str, Any]:
    """Project a CqResult into a stable snapshot payload.

    This intentionally excludes volatile detail payloads and timing-heavy
    enrichment telemetry while preserving top-level behavioral contracts.
    """
    summary = result.summary
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
    }

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
