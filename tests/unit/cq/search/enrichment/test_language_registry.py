"""Tests for language enrichment adapter registry."""

from __future__ import annotations

from collections.abc import Mapping

import tools.cq.search.enrichment.language_registry as registry
from tools.cq.core.types import QueryLanguage


class _Adapter:
    language: QueryLanguage = "python"

    @staticmethod
    def payload_from_match(match: object) -> dict[str, object] | None:
        return {"match": str(match)}

    @staticmethod
    def accumulate_telemetry(lang_bucket: dict[str, object], payload: dict[str, object]) -> None:
        lang_bucket["seen"] = payload.get("match")

    @staticmethod
    def build_diagnostics(payload: Mapping[str, object]) -> list[dict[str, object]]:
        return [dict(payload)]


def test_get_language_adapter_bootstraps_defaults() -> None:
    """Default registry bootstrap should resolve built-in python/rust adapters."""
    assert registry.get_language_adapter("python") is not None
    assert registry.get_language_adapter("rust") is not None


def test_register_language_adapter_overrides_slot() -> None:
    """Explicit registration should replace adapter for a language."""
    adapter = _Adapter()
    registry.register_language_adapter("python", adapter)

    assert registry.get_language_adapter("python") is adapter
