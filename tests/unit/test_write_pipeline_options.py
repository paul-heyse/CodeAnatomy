"""Unit tests for write pipeline option encoding."""

from __future__ import annotations

from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WriteRequest


def test_write_request_defaults() -> None:
    """Default write request to Delta format with error mode."""
    request = WriteRequest(
        source="SELECT 1 AS id",
        destination="/tmp/output",
    )
    assert request.format is WriteFormat.DELTA
    assert request.mode is WriteMode.ERROR
