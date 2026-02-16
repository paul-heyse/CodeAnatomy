"""Tests for python stage helpers."""

from __future__ import annotations

from typing import Any, cast

from tools.cq.search.python.pipeline_support import extract_signature_stage


class _Node:
    def __init__(self, text: str) -> None:
        self._text = text

    def text(self) -> str:
        return self._text


def test_extract_signature_stage_truncates_and_extracts() -> None:
    """Test extract signature stage truncates and extracts."""
    payload = extract_signature_stage(cast("Any", _Node("def build_graph(x, y):\n    pass")))
    signature = cast("str", payload["signature"])
    assert signature.startswith("def build_graph")
