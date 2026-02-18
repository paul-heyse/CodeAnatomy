"""Rust enrichment adapter implementation."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.types import QueryLanguage
from tools.cq.search._shared.enrichment_contracts import (
    RustTreeSitterEnrichmentV1,
    rust_enrichment_facts,
    rust_enrichment_payload,
)
from tools.cq.search._shared.helpers import safe_int_counter
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
    accumulate_rust_bundle_drift,
    accumulate_stage_timings,
)


class RustEnrichmentAdapter(LanguageEnrichmentPort):
    """Adapter for Rust enrichment payload handling."""

    language: QueryLanguage = "rust"

    def payload_from_match(self, match: object) -> RustEnrichmentPayload | None:
        """Extract normalized Rust enrichment payload from an enriched match.

        Returns:
        -------
        RustEnrichmentPayload | None
            Typed enrichment payload when available.
        """
        _ = self
        wrapper = getattr(match, "rust_tree_sitter", None)
        if not isinstance(wrapper, RustTreeSitterEnrichmentV1):
            return None
        raw = rust_enrichment_payload(wrapper)
        status_raw = raw.get("enrichment_status")
        sources_raw = raw.get("enrichment_sources")
        degrade_reason_raw = raw.get("degrade_reason")
        payload_size_hint_raw = raw.get("payload_size_hint")
        dropped_raw = raw.get("dropped_fields")
        truncated_raw = raw.get("truncated_fields")
        meta = (
            wrapper.meta
            if wrapper.meta is not None
            else EnrichmentMeta(
                language="rust",
                enrichment_status=coerce_enrichment_status(status_raw),
                enrichment_sources=(
                    [item for item in sources_raw if isinstance(item, str)]
                    if isinstance(sources_raw, list)
                    else []
                ),
                degrade_reason=degrade_reason_raw if isinstance(degrade_reason_raw, str) else None,
                payload_size_hint=(
                    payload_size_hint_raw if isinstance(payload_size_hint_raw, int) else None
                ),
                dropped_fields=(
                    [item for item in dropped_raw if isinstance(item, str)]
                    if isinstance(dropped_raw, list)
                    else None
                ),
                truncated_fields=(
                    [item for item in truncated_raw if isinstance(item, str)]
                    if isinstance(truncated_raw, list)
                    else None
                ),
            )
        )
        return RustEnrichmentPayload(
            meta=meta,
            facts=rust_enrichment_facts(wrapper),
            raw=raw,
        )

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: LanguageEnrichmentPayload,
    ) -> None:
        """Accumulate Rust enrichment telemetry counters into a language bucket."""
        _ = self
        if not isinstance(payload, RustEnrichmentPayload):
            return

        tags = payload.raw.get("query_pack_tags")
        if isinstance(tags, list):
            lang_bucket["query_pack_tags"] = safe_int_counter(
                lang_bucket.get("query_pack_tags")
            ) + len(tags)

        accumulate_runtime_flags(
            lang_bucket=lang_bucket,
            runtime_payload=payload.raw.get("query_runtime"),
        )
        accumulate_rust_bundle_drift(
            lang_bucket=lang_bucket,
            bundle=payload.raw.get("query_pack_bundle"),
        )
        stage_timings = lang_bucket.get("stage_timings_ms")
        stage_timings_bucket: dict[str, object]
        if isinstance(stage_timings, dict):
            stage_timings_bucket = stage_timings
        else:
            stage_timings_bucket = {}
            lang_bucket["stage_timings_ms"] = stage_timings_bucket
        accumulate_stage_timings(
            timings_bucket=stage_timings_bucket,
            stage_timings_ms=payload.raw.get("stage_timings_ms"),
        )

    def build_diagnostics(
        self,
        payload: Mapping[str, object] | LanguageEnrichmentPayload,
    ) -> list[dict[str, object]]:
        """Build normalized diagnostics rows from a Rust enrichment payload.

        Returns:
        -------
        list[dict[str, object]]
            Diagnostic rows normalized for cross-language rendering.
        """
        _ = self
        if isinstance(payload, PythonEnrichmentPayload):
            return []
        raw = payload.raw if isinstance(payload, RustEnrichmentPayload) else payload
        rows: list[dict[str, object]] = []
        degrade_events = raw.get("degrade_events")
        if isinstance(degrade_events, list):
            rows.extend(dict(item) for item in degrade_events[:16] if isinstance(item, Mapping))

        rows.extend(build_tree_sitter_diagnostic_rows(raw.get("cst_diagnostics")))
        if rows:
            return rows[:16]
        reason = string_or_none(raw.get("degrade_reason"))
        if reason is None:
            return []
        return [{"kind": "degrade_reason", "message": reason}]


__all__ = ["RustEnrichmentAdapter"]
