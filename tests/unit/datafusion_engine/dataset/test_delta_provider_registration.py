"""Tests for provider-first Delta registration flow."""

from __future__ import annotations

from pathlib import Path


def test_delta_registration_module_uses_dataset_provider_resolution() -> None:
    """Delta registration module uses provider resolution entrypoint."""
    source = Path("src/datafusion_engine/dataset/registration_delta.py").read_text(encoding="utf-8")
    assert "resolve_dataset_provider(" in source
