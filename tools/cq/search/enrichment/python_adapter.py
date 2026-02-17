"""Python enrichment adapter implementation."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.types import QueryLanguage
from tools.cq.search._shared.enrichment_contracts import (
    IncrementalEnrichmentV1,
    PythonEnrichmentV1,
    incremental_enrichment_payload,
    python_enrichment_payload,
)
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort
from tools.cq.search.enrichment.core import (
    accumulate_runtime_flags,
    build_tree_sitter_diagnostic_rows,
    string_or_none,
)
from tools.cq.search.enrichment.telemetry import (
    accumulate_stage_status,
    accumulate_stage_timings,
)


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
        python_payload = getattr(match, "python_enrichment", None)
        incremental_payload = getattr(match, "incremental_enrichment", None)
        out: dict[str, object] = {}
        if isinstance(python_payload, PythonEnrichmentV1):
            out.update(python_enrichment_payload(python_payload))
        if isinstance(incremental_payload, IncrementalEnrichmentV1):
            out["incremental"] = incremental_enrichment_payload(incremental_payload)
        return out or None

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: dict[str, object],
    ) -> None:
        """Accumulate Python enrichment telemetry counters into a language bucket."""
        _ = self
        incremental = payload.get("incremental")
        if isinstance(incremental, dict):
            stages_bucket = lang_bucket.get("stages")
            if isinstance(stages_bucket, dict):
                accumulate_stage_status(
                    stages_bucket=stages_bucket,
                    stage_status=incremental.get("stage_status"),
                )
            timings_bucket = lang_bucket.get("timings_ms")
            if isinstance(timings_bucket, dict):
                accumulate_stage_timings(
                    timings_bucket=timings_bucket,
                    stage_timings_ms=incremental.get("timings_ms"),
                )
        meta = payload.get("meta")
        if isinstance(meta, dict):
            stages_bucket = lang_bucket.get("stages")
            if isinstance(stages_bucket, dict):
                accumulate_stage_status(
                    stages_bucket=stages_bucket,
                    stage_status=meta.get("stage_status"),
                )
            timings_bucket = lang_bucket.get("timings_ms")
            if isinstance(timings_bucket, dict):
                accumulate_stage_timings(
                    timings_bucket=timings_bucket,
                    stage_timings_ms=meta.get("stage_timings_ms"),
                )
        accumulate_runtime_flags(
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
        rows.extend(build_tree_sitter_diagnostic_rows(payload.get("cst_diagnostics")))
        rows.extend(_parse_quality_diagnostics(payload.get("parse_quality")))
        rows.extend(_degrade_reason_diagnostics(payload))
        return rows[:16]


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
