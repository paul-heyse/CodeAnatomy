"""Tests for incremental enrichment provider."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.enrichment.incremental_provider import (
    IncrementalAnchorRequestV1,
    enrich_incremental_anchor,
)
from tools.cq.search.pipeline.enrichment_contracts import (
    incremental_enrichment_payload,
    parse_incremental_enrichment_mode,
)


def test_incremental_provider_ts_sym_dis(tmp_path: Path) -> None:
    """Build ts_sym_dis provider payload with both sym and dis sections."""
    file_path = tmp_path / "sample.py"
    file_path.write_text("x = 1\ndef f(a):\n    return a + x\n", encoding="utf-8")
    source = file_path.read_text(encoding="utf-8")

    payload = enrich_incremental_anchor(
        IncrementalAnchorRequestV1(
            root=tmp_path,
            file_path=file_path,
            source=source,
            line=3,
            col=13,
            match_text="x",
            mode=parse_incremental_enrichment_mode("ts_sym_dis"),
            python_payload=None,
            runtime_enrichment=False,
        )
    )
    assert payload is not None
    data = incremental_enrichment_payload(payload)
    assert data["mode"] == "ts_sym_dis"
    assert "sym" in data
    assert "dis" in data
