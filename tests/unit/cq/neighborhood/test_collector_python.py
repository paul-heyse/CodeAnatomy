"""Unit tests for Python neighborhood collector wrappers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.neighborhood.collector_python import collect_python_neighborhood
from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    TreeSitterNeighborhoodCollectResult,
)

_TOP_K = 5


def test_collect_python_neighborhood_routes_with_python_language(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collector wrapper should route with fixed `python` language value."""
    captured: list[TreeSitterNeighborhoodCollectRequest] = []

    def fake_collect(
        request: TreeSitterNeighborhoodCollectRequest,
    ) -> TreeSitterNeighborhoodCollectResult:
        captured.append(request)
        return TreeSitterNeighborhoodCollectResult()

    monkeypatch.setattr(
        "tools.cq.neighborhood.collector_python.collect_language_neighborhood",
        fake_collect,
    )

    out = collect_python_neighborhood(
        "a.py",
        source_bytes=b"print(1)\n",
        anchor_byte=0,
        root=Path(),
        target_name="print",
        top_k=_TOP_K,
    )

    assert out == TreeSitterNeighborhoodCollectResult()
    assert len(captured) == 1
    request = captured[0]
    assert request.language == "python"
    assert request.target_file == "a.py"
    assert request.target_name == "print"
    assert request.max_per_slice == _TOP_K
