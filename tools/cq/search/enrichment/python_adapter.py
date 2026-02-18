"""Python enrichment adapter implementation."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.types import QueryLanguage
from tools.cq.search._shared.enrichment_contracts import (
    IncrementalEnrichmentV1,
    PythonEnrichmentV1,
    incremental_enrichment_facts,
    incremental_enrichment_payload,
    python_enrichment_facts,
    python_enrichment_payload,
)
from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    LanguageEnrichmentPayload,
    LanguageEnrichmentPort,
    PythonEnrichmentPayload,
    RustEnrichmentPayload,
)
from tools.cq.search.enrichment.core import (
    accumulate_runtime_flags,
    build_tree_sitter_diagnostic_rows,
    coerce_enrichment_status,
    string_or_none,
)
from tools.cq.search.enrichment.telemetry import (
    accumulate_stage_status,
    accumulate_stage_timings,
)


def _to_mapping(value: object) -> dict[str, object]:
    built = msgspec.to_builtins(value, str_keys=True)
    return built if isinstance(built, dict) else {}


_META_FIELD_KEYS: tuple[str, ...] = (
    "language",
    "enrichment_status",
    "enrichment_sources",
    "degrade_reason",
    "payload_size_hint",
    "dropped_fields",
    "truncated_fields",
)


def _extract_meta_fields(payload: dict[str, object]) -> dict[str, object]:
    meta: dict[str, object] = {}
    for key in _META_FIELD_KEYS:
        if key in payload:
            meta[key] = payload.pop(key)
    return meta


def _raw_python_payload(wrapper: PythonEnrichmentV1) -> dict[str, object]:
    payload = python_enrichment_payload(wrapper)
    structure = payload.pop("structure", None)
    if isinstance(structure, Mapping):
        payload["structural"] = dict(structure)
    payload_meta = _extract_meta_fields(payload)
    meta = _to_mapping(wrapper.meta) if wrapper.meta is not None else payload_meta
    if not meta:
        meta = payload_meta
    if meta and "enrichment_status" not in meta:
        meta["enrichment_status"] = "applied"
    if meta:
        payload["meta"] = meta
    return payload


class PythonEnrichmentAdapter(LanguageEnrichmentPort):
    """Adapter for Python enrichment payload handling."""

    language: QueryLanguage = "python"

    def payload_from_match(self, match: object) -> PythonEnrichmentPayload | None:
        """Extract normalized Python enrichment payload from an enriched match.

        Returns:
        -------
        PythonEnrichmentPayload | None
            Typed enrichment payload when available.
        """
        _ = self
        python_payload = getattr(match, "python_enrichment", None)
        incremental_payload = getattr(match, "incremental_enrichment", None)
        if not isinstance(python_payload, PythonEnrichmentV1):
            return None
        raw = _raw_python_payload(python_payload)
        meta = python_payload.meta or EnrichmentMeta(language="python")
        meta = EnrichmentMeta(
            language=meta.language,
            enrichment_status=coerce_enrichment_status(meta.enrichment_status),
            enrichment_sources=list(meta.enrichment_sources),
            degrade_reason=meta.degrade_reason,
            payload_size_hint=meta.payload_size_hint,
            dropped_fields=list(meta.dropped_fields) if meta.dropped_fields is not None else None,
            truncated_fields=(
                list(meta.truncated_fields) if meta.truncated_fields is not None else None
            ),
        )
        if isinstance(incremental_payload, IncrementalEnrichmentV1):
            raw["incremental"] = incremental_enrichment_payload(incremental_payload)
            incremental_facts = incremental_enrichment_facts(incremental_payload)
        else:
            incremental_facts = None
        return PythonEnrichmentPayload(
            meta=meta,
            facts=python_enrichment_facts(python_payload),
            incremental=incremental_facts,
            raw=raw,
        )

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: LanguageEnrichmentPayload,
    ) -> None:
        """Accumulate Python enrichment telemetry counters into a language bucket."""
        _ = self
        if not isinstance(payload, PythonEnrichmentPayload):
            return
        incremental = payload.incremental
        if incremental is not None:
            stages_bucket = lang_bucket.get("stages")
            if isinstance(stages_bucket, dict):
                accumulate_stage_status(
                    stages_bucket=stages_bucket,
                    stage_status=incremental.stage_status,
                )
            timings_bucket = lang_bucket.get("timings_ms")
            if isinstance(timings_bucket, dict):
                accumulate_stage_timings(
                    timings_bucket=timings_bucket,
                    stage_timings_ms=incremental.timings_ms,
                )
        meta = payload.raw.get("meta")
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
            runtime_payload=payload.raw.get("query_runtime"),
        )

    def build_diagnostics(
        self,
        payload: Mapping[str, object] | LanguageEnrichmentPayload,
    ) -> list[dict[str, object]]:
        """Build normalized diagnostics rows from a Python enrichment payload.

        Returns:
        -------
        list[dict[str, object]]
            Diagnostic rows normalized for cross-language rendering.
        """
        _ = self
        if isinstance(payload, RustEnrichmentPayload):
            return []
        raw = payload.raw if isinstance(payload, PythonEnrichmentPayload) else payload
        rows: list[dict[str, object]] = []
        rows.extend(build_tree_sitter_diagnostic_rows(raw.get("cst_diagnostics")))
        rows.extend(_parse_quality_diagnostics(raw.get("parse_quality")))
        rows.extend(_degrade_reason_diagnostics(raw))
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
