"""Tests for python resolution index wrappers."""

from __future__ import annotations

import pytest
from tools.cq.search.python import resolution_index as resolution_index_module


def test_build_resolution_index_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        resolution_index_module,
        "enrich_python_resolution_by_byte_range",
        lambda *_args, **_kwargs: {"symbol": "foo"},
    )
    payload = resolution_index_module.build_resolution_index(
        source_bytes=b"def foo():\n    pass\n",
        byte_start=0,
        byte_end=5,
        cache_key="x.py",
    )
    assert payload["symbol"] == "foo"
