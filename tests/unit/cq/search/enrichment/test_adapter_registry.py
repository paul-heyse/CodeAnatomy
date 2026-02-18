"""Tests for enrichment adapter registry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.adapter_registry import LanguageAdapterRegistry
from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    LanguageEnrichmentPayload,
    PythonEnrichmentPayload,
)


@dataclass
class _Adapter:
    language: QueryLanguage

    @staticmethod
    def payload_from_match(match: object) -> LanguageEnrichmentPayload | None:
        del match
        return PythonEnrichmentPayload(meta=EnrichmentMeta(language="python"), raw={})

    @staticmethod
    def accumulate_telemetry(
        lang_bucket: dict[str, object],
        payload: LanguageEnrichmentPayload,
    ) -> None:
        lang_bucket.update(payload.raw)

    @staticmethod
    def build_diagnostics(
        payload: Mapping[str, object] | LanguageEnrichmentPayload,
    ) -> list[dict[str, object]]:
        if isinstance(payload, Mapping):
            return [dict(payload)]
        return [dict(payload.raw)]


def test_adapter_registry_register_and_get() -> None:
    """Adapter registry should store and retrieve adapters by language."""
    registry = LanguageAdapterRegistry()
    adapter = _Adapter(language="python")

    registry.register("python", adapter)

    assert registry.get("python") is adapter


def test_adapter_registry_clear() -> None:
    """Adapter registry clear should remove previously registered adapters."""
    registry = LanguageAdapterRegistry()
    registry.register("python", _Adapter(language="python"))

    registry.clear()

    assert registry.get("python") is None
