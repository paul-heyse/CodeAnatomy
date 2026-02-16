# ruff: noqa: D100, D103, INP001, PLR2004
from __future__ import annotations

import pytest

from datafusion_engine.session.hooks import chain_optional_hooks, record_registration


def test_chain_optional_hooks_returns_none_for_empty_hooks() -> None:
    assert chain_optional_hooks(None, None) is None


def test_chain_optional_hooks_invokes_hooks_in_order() -> None:
    events: list[str] = []

    def first(value: str) -> None:
        events.append(f"first:{value}")

    def second(value: str) -> None:
        events.append(f"second:{value}")

    chained = chain_optional_hooks(first, None, second)
    assert chained is not None
    chained("payload")
    assert events == ["first:payload", "second:payload"]


def test_record_registration_rejects_duplicates_by_default() -> None:
    registry: dict[str, int] = {"x": 1}
    with pytest.raises(ValueError, match="Duplicate test registration"):
        record_registration(registry, "x", 2, category="test")


def test_record_registration_allows_override_when_requested() -> None:
    registry: dict[str, int] = {"x": 1}
    record_registration(registry, "x", 2, category="test", allow_override=True)
    assert registry["x"] == 2
