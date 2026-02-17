"""Parity checks between config spec and runtime model fields."""

from __future__ import annotations

from core.config_specs import RootConfigSpec
from runtime_models.root import RootConfigRuntime


def test_config_spec_field_parity() -> None:
    """Spec and runtime models expose the same top-level field names."""
    spec_fields = set(RootConfigSpec.__struct_fields__)
    runtime_fields = set(RootConfigRuntime.model_fields)
    assert spec_fields == runtime_fields
