"""Tests for function registry merging and lane selection."""

from __future__ import annotations

from engine.function_registry import default_function_registry


def test_function_registry_uses_rust_only_lane() -> None:
    """Use the Rust-only lane for UDF execution."""
    registry = default_function_registry()
    spec = registry.specs["stable_hash64"]
    assert spec.lanes == ("df_rust",)


def test_function_registry_resolve_lane_prefers_datafusion() -> None:
    """Resolve the preferred lane using registry precedence."""
    registry = default_function_registry()
    assert registry.resolve_lane("stable_hash64") == "df_rust"
