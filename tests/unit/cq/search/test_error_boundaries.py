"""Tests for shared enrichment error boundaries."""

from __future__ import annotations

from tools.cq.search._shared.error_boundaries import ENRICHMENT_ERRORS


def test_enrichment_error_boundaries_are_canonical() -> None:
    """Enrichment errors stay aligned with the canonical shared tuple."""
    assert (
        RuntimeError,
        TypeError,
        ValueError,
        AttributeError,
        UnicodeError,
        IndexError,
        KeyError,
        OSError,
    ) == ENRICHMENT_ERRORS
