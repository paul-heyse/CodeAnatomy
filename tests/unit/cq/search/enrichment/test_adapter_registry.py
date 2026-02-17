"""Tests for enrichment adapter registry."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.enrichment.adapter_registry import LanguageAdapterRegistry


@dataclass
class _Adapter:
    language: str


def test_adapter_registry_register_and_get() -> None:
    """Adapter registry should store and retrieve adapters by language."""
    registry = LanguageAdapterRegistry()
    adapter = _Adapter(language="python")

    registry.register("python", adapter)  # type: ignore[arg-type]

    assert registry.get("python") is adapter


def test_adapter_registry_clear() -> None:
    """Adapter registry clear should remove previously registered adapters."""
    registry = LanguageAdapterRegistry()
    registry.register("python", _Adapter(language="python"))  # type: ignore[arg-type]

    registry.clear()

    assert registry.get("python") is None
