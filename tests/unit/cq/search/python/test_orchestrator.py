"""Tests for python orchestrator wrapper."""

from __future__ import annotations

from typing import cast

import pytest
from tools.cq.search._shared.core import PythonByteRangeEnrichmentRequest
from tools.cq.search.python import orchestrator as orchestrator_module


def test_run_python_enrichment_pipeline_merges_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        orchestrator_module,
        "extract_python_byte_range",
        lambda _request: {"a": 1},
    )
    monkeypatch.setattr(
        orchestrator_module,
        "build_resolution_index",
        lambda **_kwargs: {"symbol": "foo"},
    )
    monkeypatch.setattr(
        orchestrator_module,
        "evaluate_python_semantic_signal_from_mapping",
        lambda _payload: (True, ("ok",)),
    )

    request = PythonByteRangeEnrichmentRequest(
        sg_root=object(),
        source_bytes=b"def foo():\n    pass\n",
        byte_start=0,
        byte_end=3,
        cache_key="x.py",
    )
    payload = orchestrator_module.run_python_enrichment_pipeline(request)
    resolution = cast("dict[str, object]", payload["resolution"])
    semantic_signal = cast("dict[str, object]", payload["semantic_signal"])
    assert resolution["symbol"] == "foo"
    assert semantic_signal["has_signal"] is True
