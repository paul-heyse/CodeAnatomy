"""Tests for language enrichment adapter registry."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import pytest
import tools.cq.search.enrichment.language_registry as registry
from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.adapter_registry import LanguageAdapterRegistry
from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    LanguageEnrichmentPayload,
    PythonEnrichmentPayload,
)


class _Adapter:
    language: QueryLanguage = "python"

    @staticmethod
    def payload_from_match(match: object) -> LanguageEnrichmentPayload | None:
        return PythonEnrichmentPayload(
            meta=EnrichmentMeta(language="python"),
            raw={"match": str(match)},
        )

    @staticmethod
    def accumulate_telemetry(
        lang_bucket: dict[str, object],
        payload: LanguageEnrichmentPayload,
    ) -> None:
        lang_bucket["seen"] = payload.raw.get("match")

    @staticmethod
    def build_diagnostics(
        payload: Mapping[str, object] | LanguageEnrichmentPayload,
    ) -> list[dict[str, object]]:
        if isinstance(payload, Mapping):
            return [dict(payload)]
        return [dict(payload.raw)]


@pytest.fixture(autouse=True)
def _reset_default_registry() -> Iterator[None]:
    registry.set_default_adapter_registry(None)
    yield
    registry.set_default_adapter_registry(None)


def test_get_language_adapter_bootstraps_defaults() -> None:
    """Default registry bootstrap should resolve built-in python/rust adapters."""
    assert registry.get_language_adapter("python") is not None
    assert registry.get_language_adapter("rust") is not None


def test_register_language_adapter_overrides_slot() -> None:
    """Explicit registration should replace adapter for a language."""
    adapter = _Adapter()
    registry.register_language_adapter("python", adapter)

    assert registry.get_language_adapter("python") is adapter


def test_set_default_adapter_registry_uses_injected_registry() -> None:
    """Injected default registry should be used by helper accessors."""
    injected = LanguageAdapterRegistry()
    adapter = _Adapter()
    injected.register("python", adapter)
    registry.set_default_adapter_registry(injected)

    assert registry.get_language_adapter("python") is adapter
