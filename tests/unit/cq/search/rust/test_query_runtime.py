"""Tests for Rust runtime helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.rust import enrichment as runtime_module


def test_runtime_available_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_module, "is_tree_sitter_rust_available", lambda: True)
    assert runtime_module.runtime_available() is True


def test_enrich_context_by_byte_range_normalizes_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_module,
        "enrich_rust_context_by_byte_range",
        lambda *_args, **_kwargs: None,
    )
    payload = runtime_module.enrich_context_by_byte_range("fn main(){}", byte_start=0, byte_end=2)
    assert payload == {}
