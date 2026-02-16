# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from inspect import signature

from datafusion_engine.io.write import WritePipeline


def test_write_pipeline_signature_removes_prefer_streaming() -> None:
    assert "prefer_streaming" not in signature(WritePipeline.write).parameters
    assert "prefer_streaming" not in signature(WritePipeline.write_view).parameters
