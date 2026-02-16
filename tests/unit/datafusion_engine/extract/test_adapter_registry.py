"""Unit tests for extract adapter metadata helpers."""

from __future__ import annotations

from datafusion_engine.extract import adapter_registry
from datafusion_engine.extract.adapter_registry import (
    adapter_executor_key,
    extract_template_adapters,
)


def test_adapter_executor_key_is_identity_for_registered_adapters() -> None:
    """Test adapter executor key is identity for registered adapters."""
    names = [adapter.name for adapter in extract_template_adapters()]
    assert names
    assert {adapter_executor_key(name) for name in names} == set(names)


def test_adapter_executor_key_is_exported() -> None:
    """Test adapter executor key is exported."""
    assert "adapter_executor_key" in adapter_registry.__all__
