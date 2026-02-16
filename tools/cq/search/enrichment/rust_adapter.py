"""Rust enrichment adapter implementation."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.query.language import QueryLanguage
from tools.cq.search.enrichment.contracts import LanguageEnrichmentPort
from tools.cq.search.enrichment.core import string_or_none


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
        return dict(payload) if isinstance(payload, dict) else None

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: dict[str, object],
    ) -> None:
        """Accumulate Rust enrichment telemetry counters into a language bucket."""
        _ = self

        tags = payload.get("query_pack_tags")
        if isinstance(tags, list):
            lang_bucket["query_pack_tags"] = _counter(lang_bucket.get("query_pack_tags")) + len(
                tags
            )

        _accumulate_runtime_flags(
            lang_bucket=lang_bucket,
            runtime_payload=payload.get("query_runtime"),
        )
        _accumulate_bundle_drift(
            lang_bucket=lang_bucket,
            bundle=payload.get("query_pack_bundle"),
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

        tree_sitter_rows = payload.get("cst_diagnostics")
        if isinstance(tree_sitter_rows, list):
            rows.extend(
                {
                    "kind": string_or_none(item.get("kind")) or "tree_sitter",
                    "message": string_or_none(item.get("message")) or "tree-sitter diagnostic",
                    "line": item.get("start_line"),
                    "col": item.get("start_col"),
                }
                for item in tree_sitter_rows[:8]
                if isinstance(item, Mapping)
            )
        if rows:
            return rows[:16]
        reason = string_or_none(payload.get("degrade_reason"))
        if reason is None:
            return []
        return [{"kind": "degrade_reason", "message": reason}]


def _counter(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _accumulate_runtime_flags(*, lang_bucket: dict[str, object], runtime_payload: object) -> None:
    runtime_bucket = lang_bucket.get("query_runtime")
    if not isinstance(runtime_payload, dict) or not isinstance(runtime_bucket, dict):
        return
    if bool(runtime_payload.get("did_exceed_match_limit")):
        runtime_bucket["did_exceed_match_limit"] = (
            _counter(runtime_bucket.get("did_exceed_match_limit")) + 1
        )
    if bool(runtime_payload.get("cancelled")):
        runtime_bucket["cancelled"] = _counter(runtime_bucket.get("cancelled")) + 1


def _accumulate_bundle_drift(*, lang_bucket: dict[str, object], bundle: object) -> None:
    if not isinstance(bundle, dict):
        return
    if bool(bundle.get("distribution_included")):
        lang_bucket["distribution_profile_hits"] = (
            _counter(lang_bucket.get("distribution_profile_hits")) + 1
        )
    if bundle.get("drift_compatible") is False:
        lang_bucket["drift_breaking_profile_hits"] = (
            _counter(lang_bucket.get("drift_breaking_profile_hits")) + 1
        )
    schema_diff = bundle.get("drift_schema_diff")
    if not isinstance(schema_diff, dict):
        return
    removed_nodes = schema_diff.get("removed_node_kinds")
    removed_fields = schema_diff.get("removed_fields")
    if isinstance(removed_nodes, list):
        lang_bucket["drift_removed_node_kinds"] = _counter(
            lang_bucket.get("drift_removed_node_kinds")
        ) + len(removed_nodes)
    if isinstance(removed_fields, list):
        lang_bucket["drift_removed_fields"] = _counter(
            lang_bucket.get("drift_removed_fields")
        ) + len(removed_fields)


__all__ = ["RustEnrichmentAdapter"]
