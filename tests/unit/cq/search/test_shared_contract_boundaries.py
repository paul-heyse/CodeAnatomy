"""Tests for shared serialization/runtime boundary contracts."""

from __future__ import annotations

import pytest
from tools.cq.search._shared.core import (
    PythonByteRangeEnrichmentRequest,
    PythonNodeEnrichmentRequest,
    assert_no_runtime_only_keys,
    has_runtime_only_keys,
)

QUERY_BUDGET_MS = 25


def test_python_node_request_splits_settings_and_runtime() -> None:
    """Test python node request splits settings and runtime."""
    request = PythonNodeEnrichmentRequest(
        sg_root=object(),
        node=object(),
        source_bytes=b"print('x')\n",
        line=1,
        col=0,
        cache_key="k",
        query_budget_ms=QUERY_BUDGET_MS,
        session=object(),
    )

    settings = request.to_settings()
    runtime = request.to_runtime()

    assert settings.cache_key == "k"
    assert settings.query_budget_ms == QUERY_BUDGET_MS
    assert runtime.sg_root is request.sg_root
    assert runtime.node is request.node


def test_python_byte_range_request_splits_settings_and_runtime() -> None:
    """Test python byte range request splits settings and runtime."""
    request = PythonByteRangeEnrichmentRequest(
        sg_root=object(),
        source_bytes=b"print('x')\n",
        byte_start=0,
        byte_end=5,
        cache_key="k2",
        resolved_node=object(),
    )

    settings = request.to_settings()
    runtime = request.to_runtime()

    assert settings.cache_key == "k2"
    assert settings.byte_start == 0
    assert runtime.sg_root is request.sg_root
    assert runtime.resolved_node is request.resolved_node


def test_runtime_boundary_assertion_rejects_runtime_keys() -> None:
    """Test runtime boundary assertion rejects runtime keys."""
    payload = {"sg_root": object(), "cache_key": "k"}
    assert has_runtime_only_keys(payload)
    with pytest.raises(TypeError):
        assert_no_runtime_only_keys(payload)
