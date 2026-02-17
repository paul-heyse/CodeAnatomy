"""Tests for incremental enrichment contracts."""

from __future__ import annotations

from tools.cq.search._shared.enrichment_contracts import (
    IncrementalEnrichmentModeV1,
    IncrementalEnrichmentV1,
    incremental_enrichment_payload,
    parse_incremental_enrichment_mode,
    wrap_incremental_enrichment,
)


def test_parse_incremental_mode_from_string() -> None:
    """Parse incremental mode strings with fallback to default mode."""
    assert parse_incremental_enrichment_mode("full") is IncrementalEnrichmentModeV1.FULL
    assert parse_incremental_enrichment_mode("bad") is IncrementalEnrichmentModeV1.TS_SYM


def test_wrap_incremental_payload_contract() -> None:
    """Wrap a raw payload into the incremental enrichment contract."""
    wrapped = wrap_incremental_enrichment(
        {"scope_graph": {"tables_count": 2}},
        mode=IncrementalEnrichmentModeV1.TS_SYM,
    )
    assert isinstance(wrapped, IncrementalEnrichmentV1)
    payload = incremental_enrichment_payload(wrapped)
    assert payload["mode"] == "ts_sym"
    assert payload["scope_graph"] == {"tables_count": 2}
