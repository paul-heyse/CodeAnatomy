"""Tests for Rust enrichment adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from tools.cq.search._shared.enrichment_contracts import RustTreeSitterEnrichmentV1
from tools.cq.search.enrichment.contracts import EnrichmentMeta, RustEnrichmentPayload
from tools.cq.search.enrichment.rust_adapter import RustEnrichmentAdapter

_TAG_COUNT = 2
_REMOVED_FIELD_COUNT = 2


@dataclass(frozen=True)
class _Match:
    rust_tree_sitter: object


def test_payload_from_match_extracts_rust_payload() -> None:
    """Adapter should extract rust_tree_sitter mapping from match object."""
    adapter = RustEnrichmentAdapter()
    payload = adapter.payload_from_match(
        _Match(RustTreeSitterEnrichmentV1(meta=EnrichmentMeta(language="rust")))
    )

    assert payload is not None
    assert payload.meta.language == "rust"


def test_accumulate_telemetry_updates_drift_and_runtime_counters() -> None:
    """Rust telemetry aggregation should increment runtime and drift counters."""
    adapter = RustEnrichmentAdapter()
    bucket: dict[str, object] = {
        "query_pack_tags": 0,
        "query_runtime": {"did_exceed_match_limit": 0, "cancelled": 0},
        "distribution_profile_hits": 0,
        "drift_breaking_profile_hits": 0,
        "drift_removed_node_kinds": 0,
        "drift_removed_fields": 0,
    }
    payload = RustEnrichmentPayload(
        meta=EnrichmentMeta(language="rust"),
        raw={
            "query_pack_tags": ["defs", "calls"],
            "query_runtime": {"did_exceed_match_limit": True, "cancelled": True},
            "query_pack_bundle": {
                "distribution_included": True,
                "drift_compatible": False,
                "drift_schema_diff": {
                    "removed_node_kinds": ["x"],
                    "removed_fields": ["y", "z"],
                },
            },
        },
    )

    adapter.accumulate_telemetry(bucket, payload)

    assert bucket["query_pack_tags"] == _TAG_COUNT
    runtime = cast("dict[str, dict[str, int]]", {"root": bucket["query_runtime"]})["root"]
    assert runtime["did_exceed_match_limit"] == 1
    assert runtime["cancelled"] == 1
    assert bucket["distribution_profile_hits"] == 1
    assert bucket["drift_breaking_profile_hits"] == 1
    assert bucket["drift_removed_node_kinds"] == 1
    assert bucket["drift_removed_fields"] == _REMOVED_FIELD_COUNT


def test_build_diagnostics_prefers_degrade_events() -> None:
    """Rust diagnostics should include degrade events when provided."""
    adapter = RustEnrichmentAdapter()
    rows = adapter.build_diagnostics(
        {
            "degrade_events": [{"kind": "timeout", "message": "query timed out"}],
            "degrade_reason": "fallback",
        }
    )

    assert rows
    assert rows[0]["kind"] == "timeout"
