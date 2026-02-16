"""Tests for python resolution payload contracts."""

from __future__ import annotations

from tools.cq.search.python.pipeline_support import (
    PythonResolutionPayloadV1,
    coerce_resolution_payload,
)


def test_coerce_resolution_payload() -> None:
    """Test coerce resolution payload."""
    payload = coerce_resolution_payload(
        {
            "symbol": "foo",
            "symbol_role": "definition",
            "qualified_name_candidates": ({"name": "pkg.foo"},),
        }
    )
    assert isinstance(payload, PythonResolutionPayloadV1)
    assert payload.symbol == "foo"
