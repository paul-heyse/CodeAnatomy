"""Unit tests for Python neighborhood collector wrappers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.neighborhood.collector_python import collect_python_neighborhood


def test_collect_python_neighborhood_routes_with_python_language(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collector wrapper should route with fixed `python` language value."""
    captured: dict[str, object] = {}

    def fake_collect(**kwargs: object) -> str:
        captured.update(kwargs)
        return "ok"

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
        top_k=5,
    )

    assert out == "ok"
    assert captured["language"] == "python"
