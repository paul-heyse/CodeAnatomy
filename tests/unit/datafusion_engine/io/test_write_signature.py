"""Tests for public write pipeline signatures."""

from __future__ import annotations

from inspect import signature

from datafusion_engine.io.write_core import WritePipeline


def test_write_pipeline_signature_removes_prefer_streaming() -> None:
    """Write signatures no longer include legacy prefer_streaming option."""
    assert "prefer_streaming" not in signature(WritePipeline.write).parameters
    assert "prefer_streaming" not in signature(WritePipeline.write_view).parameters
