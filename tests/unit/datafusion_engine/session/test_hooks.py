"""Tests for hook chaining and registration helpers."""

from __future__ import annotations

import pytest

from datafusion_engine.session.hooks import chain_optional_hooks, record_registration

EXPECTED_HOOK_EVENT_COUNT = 2
OVERRIDE_REGISTRY_VALUE = 2


def test_chain_optional_hooks_returns_none_for_empty_hooks() -> None:
    """No hooks yields no chained callback."""
    assert chain_optional_hooks(None, None) is None


def test_chain_optional_hooks_invokes_hooks_in_order() -> None:
    """Hooks execute in declaration order."""
    events: list[str] = []

    def first(value: str) -> None:
        events.append(f"first:{value}")

    def second(value: str) -> None:
        events.append(f"second:{value}")

    chained = chain_optional_hooks(first, None, second)
    assert chained is not None
    chained("payload")
    assert len(events) == EXPECTED_HOOK_EVENT_COUNT
    assert events == ["first:payload", "second:payload"]


def test_record_registration_rejects_duplicates_by_default() -> None:
    """Duplicate registration raises when override is disabled."""
    registry: dict[str, int] = {"x": 1}
    with pytest.raises(ValueError, match="Duplicate test registration"):
        record_registration(registry, "x", 2, category="test")


def test_record_registration_allows_override_when_requested() -> None:
    """Override mode allows replacing existing registrations."""
    registry: dict[str, int] = {"x": 1}
    record_registration(
        registry,
        "x",
        OVERRIDE_REGISTRY_VALUE,
        category="test",
        allow_override=True,
    )
    assert registry["x"] == OVERRIDE_REGISTRY_VALUE
    assert len(registry) == 1
