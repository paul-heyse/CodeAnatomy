"""Tests for Rust extraction helpers."""

from __future__ import annotations

import pytest
from tools.cq.search._shared.core import RustEnrichmentRequest
from tools.cq.search.rust import enrichment as enrichment_module


def test_extract_rust_context_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test extract rust context delegates."""
    monkeypatch.setattr(
        enrichment_module,
        "enrich_rust_context_by_byte_range",
        lambda *_args, **_kwargs: {"language": "rust"},
    )
    request = RustEnrichmentRequest(source="fn main(){}", byte_start=0, byte_end=2)
    payload = enrichment_module.extract_rust_context(request)
    assert payload["language"] == "rust"
