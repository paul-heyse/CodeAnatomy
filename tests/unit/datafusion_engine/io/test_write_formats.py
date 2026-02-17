"""Tests for write format helper utilities."""

from __future__ import annotations

from datafusion_engine.io.write_formats import KNOWN_WRITE_FORMATS, is_known_write_format


def test_known_write_formats_are_detected() -> None:
    """Known write formats are recognized correctly."""
    assert "delta" in KNOWN_WRITE_FORMATS
    assert is_known_write_format("Parquet") is True
    assert is_known_write_format("unknown") is False
