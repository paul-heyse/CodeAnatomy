"""Rust enrichment adapter implementation."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.types import QueryLanguage
from tools.cq.search._shared.enrichment_contracts import (
    RustTreeSitterEnrichmentV1,
    rust_enrichment_payload,
)
from tools.cq.search._shared.helpers import safe_int_counter
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort
from tools.cq.search.enrichment.core import (
    accumulate_runtime_flags,
    build_tree_sitter_diagnostic_rows,
    string_or_none,
)
from tools.cq.search.enrichment.telemetry import (
    accumulate_rust_bundle_drift,
    accumulate_stage_timings,
)


class RustEnrichmentAdapter(LanguageEnrichmentPort):
    """Adapter for Rust enrichment payload handling."""

    language: QueryLanguage = "rust"

    def payload_from_match(self, match: object) -> dict[str, object] | None:
        """Extract normalized Rust enrichment payload from an enriched match.

        Returns:
        -------
        dict[str, object] | None
            Enrichment payload mapping when available, otherwise ``None``.
        """
        _ = self
        payload = getattr(match, "rust_tree_sitter", None)
        if not isinstance(payload, RustTreeSitterEnrichmentV1):
            return None
        return rust_enrichment_payload(payload)

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: dict[str, object],
    ) -> None:
        """Accumulate Rust enrichment telemetry counters into a language bucket."""
        _ = self

        tags = payload.get("query_pack_tags")
        if isinstance(tags, list):
            lang_bucket["query_pack_tags"] = safe_int_counter(
                lang_bucket.get("query_pack_tags")
            ) + len(tags)

        accumulate_runtime_flags(
            lang_bucket=lang_bucket,
            runtime_payload=payload.get("query_runtime"),
        )
        accumulate_rust_bundle_drift(
            lang_bucket=lang_bucket,
            bundle=payload.get("query_pack_bundle"),
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
            stage_timings_ms=payload.get("stage_timings_ms"),
        )

    def build_diagnostics(self, payload: Mapping[str, object]) -> list[dict[str, object]]:
        """Build normalized diagnostics rows from a Rust enrichment payload.

        Returns:
        -------
        list[dict[str, object]]
            Diagnostic rows normalized for cross-language rendering.
        """
        _ = self
        rows: list[dict[str, object]] = []
        degrade_events = payload.get("degrade_events")
        if isinstance(degrade_events, list):
            rows.extend(dict(item) for item in degrade_events[:16] if isinstance(item, Mapping))

        rows.extend(build_tree_sitter_diagnostic_rows(payload.get("cst_diagnostics")))
        if rows:
            return rows[:16]
        reason = string_or_none(payload.get("degrade_reason"))
        if reason is None:
            return []
        return [{"kind": "degrade_reason", "message": reason}]


__all__ = ["RustEnrichmentAdapter"]
