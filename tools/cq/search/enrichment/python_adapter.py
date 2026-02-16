"""Python enrichment adapter implementation."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.query.language import QueryLanguage
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort
from tools.cq.search.enrichment.core import string_or_none


class PythonEnrichmentAdapter(LanguageEnrichmentPort):
    """Adapter for Python enrichment payload handling."""

    language: QueryLanguage = "python"

    def payload_from_match(self, match: object) -> dict[str, object] | None:
        """Extract normalized Python enrichment payload from an enriched match.

        Returns:
        -------
        dict[str, object] | None
            Enrichment payload mapping when available, otherwise ``None``.
        """
        _ = self
        payload = getattr(match, "python_enrichment", None)
        return dict(payload) if isinstance(payload, dict) else None

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: dict[str, object],
    ) -> None:
        """Accumulate Python enrichment telemetry counters into a language bucket."""
        _ = self
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            return
        _accumulate_stage_status(
            lang_bucket=lang_bucket,
            stage_status=meta.get("stage_status"),
        )
        _accumulate_stage_timings(
            lang_bucket=lang_bucket,
            stage_timings=meta.get("stage_timings_ms"),
        )
        _accumulate_runtime_flags(
            lang_bucket=lang_bucket,
            runtime_payload=payload.get("query_runtime"),
        )

    def build_diagnostics(self, payload: Mapping[str, object]) -> list[dict[str, object]]:
        """Build normalized diagnostics rows from a Python enrichment payload.

        Returns:
        -------
        list[dict[str, object]]
            Diagnostic rows normalized for cross-language rendering.
        """
        _ = self
        rows: list[dict[str, object]] = []
        rows.extend(_tree_sitter_diagnostics(payload.get("cst_diagnostics")))
        rows.extend(_parse_quality_diagnostics(payload.get("parse_quality")))
        rows.extend(_degrade_reason_diagnostics(payload))
        return rows[:16]


def _accumulate_stage_status(*, lang_bucket: dict[str, object], stage_status: object) -> None:
    stages_bucket = lang_bucket.get("stages")
    if not isinstance(stage_status, dict) or not isinstance(stages_bucket, dict):
        return
    for stage, stage_state in stage_status.items():
        if not isinstance(stage, str) or not isinstance(stage_state, str):
            continue
        stage_bucket = stages_bucket.get(stage)
        if not isinstance(stage_bucket, dict):
            continue
        if stage_state not in {"applied", "degraded", "skipped"}:
            continue
        stage_bucket[stage_state] = int(stage_bucket.get(stage_state, 0)) + 1


def _accumulate_stage_timings(*, lang_bucket: dict[str, object], stage_timings: object) -> None:
    timings_bucket = lang_bucket.get("timings_ms")
    if not isinstance(stage_timings, dict) or not isinstance(timings_bucket, dict):
        return
    for stage, stage_ms in stage_timings.items():
        if not isinstance(stage, str) or not isinstance(stage_ms, (int, float)):
            continue
        base = timings_bucket.get(stage)
        base_ms = float(base) if isinstance(base, (int, float)) else 0.0
        timings_bucket[stage] = base_ms + float(stage_ms)


def _accumulate_runtime_flags(*, lang_bucket: dict[str, object], runtime_payload: object) -> None:
    runtime_bucket = lang_bucket.get("query_runtime")
    if not isinstance(runtime_payload, dict) or not isinstance(runtime_bucket, dict):
        return
    if bool(runtime_payload.get("did_exceed_match_limit")):
        runtime_bucket["did_exceed_match_limit"] = (
            int(runtime_bucket.get("did_exceed_match_limit", 0)) + 1
        )
    if bool(runtime_payload.get("cancelled")):
        runtime_bucket["cancelled"] = int(runtime_bucket.get("cancelled", 0)) + 1


def _tree_sitter_diagnostics(rows: object) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        return []
    return [
        {
            "kind": string_or_none(item.get("kind")) or "tree_sitter",
            "message": string_or_none(item.get("message")) or "tree-sitter diagnostic",
            "line": item.get("start_line"),
            "col": item.get("start_col"),
        }
        for item in rows[:8]
        if isinstance(item, Mapping)
    ]


def _parse_quality_diagnostics(parse_quality: object) -> list[dict[str, object]]:
    if not isinstance(parse_quality, Mapping):
        return []
    diagnostics: list[dict[str, object]] = []
    for kind in ("error_nodes", "missing_nodes"):
        values = parse_quality.get(kind)
        if not isinstance(values, list):
            continue
        for item in values[:8]:
            text = string_or_none(item)
            if text is not None:
                diagnostics.append({"kind": kind, "message": text})
    return diagnostics


def _degrade_reason_diagnostics(payload: Mapping[str, object]) -> list[dict[str, object]]:
    diagnostics: list[dict[str, object]] = []
    reasons = payload.get("degrade_reasons")
    if isinstance(reasons, list):
        for reason in reasons[:8]:
            text = string_or_none(reason)
            if text is not None:
                diagnostics.append({"kind": "degrade_reason", "message": text})
    reason = string_or_none(payload.get("degrade_reason"))
    if reason is not None:
        diagnostics.append({"kind": "degrade_reason", "message": reason})
    return diagnostics


__all__ = ["PythonEnrichmentAdapter"]
