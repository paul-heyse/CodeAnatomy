"""Tests for registry protocol utilities."""

from __future__ import annotations

import pytest

from utils.registry_protocol import ImmutableRegistry, MutableRegistry, Registry


def test_mutable_registry_register_and_get() -> None:
    """Ensure MutableRegistry supports basic register/get behavior."""
    expected_count = 2
    registry: MutableRegistry[str, int] = MutableRegistry()
    registry.register("alpha", 1)
    registry.register("beta", 2)
    assert registry.get("alpha") == 1
    assert registry.get("missing") is None
    assert "alpha" in registry
    assert "missing" not in registry
    assert len(registry) == expected_count
    assert list(iter(registry)) == ["alpha", "beta"]


def test_mutable_registry_overwrite_guard() -> None:
    """Ensure MutableRegistry enforces overwrite behavior."""
    registry: MutableRegistry[str, int] = MutableRegistry()
    expected_value = 2
    registry.register("alpha", 1)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("alpha", 2)
    registry.register("alpha", 2, overwrite=True)
    assert registry.get("alpha") == expected_value


def test_mutable_registry_snapshot_and_items() -> None:
    """Ensure MutableRegistry snapshots and items reflect entries."""
    registry: MutableRegistry[str, int] = MutableRegistry()
    registry.register("alpha", 1)
    registry.register("beta", 2)
    snapshot = registry.snapshot()
    assert snapshot == {"alpha": 1, "beta": 2}
    assert list(registry.items()) == [("alpha", 1), ("beta", 2)]


def test_immutable_registry_lookup() -> None:
    """Ensure ImmutableRegistry supports lookup and iteration."""
    expected_count = 2
    registry = ImmutableRegistry.from_dict({"alpha": 1, "beta": 2})
    assert registry.get("alpha") == 1
    assert registry.get("missing") is None
    assert "beta" in registry
    assert len(registry) == expected_count
    assert list(iter(registry)) == ["alpha", "beta"]


def test_registry_protocol_runtime_check() -> None:
    """Ensure MutableRegistry satisfies the Registry protocol at runtime."""
    registry: MutableRegistry[str, int] = MutableRegistry()
    assert isinstance(registry, Registry)
