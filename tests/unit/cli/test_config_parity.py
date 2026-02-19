"""Parity checks for top-level config spec fields."""

from __future__ import annotations

from core.config_specs import RootConfigSpec


def test_config_spec_field_parity() -> None:
    """Root config spec should expose the canonical top-level sections."""
    assert set(RootConfigSpec.__struct_fields__) == {
        "cache",
        "datafusion_cache",
        "delta",
        "docstrings",
        "engine",
        "incremental",
        "otel",
        "plan",
    }
