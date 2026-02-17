"""Typed enrichment contracts shared across Python and Rust pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol

import msgspec

from tools.cq.core.structs import CqOutputStruct
from tools.cq.core.types import QueryLanguage

EnrichmentStatus = Literal["applied", "degraded", "skipped"]


class EnrichmentMeta(CqOutputStruct, frozen=True):
    """Common enrichment metadata."""

    language: str
    enrichment_status: EnrichmentStatus = "applied"
    enrichment_sources: list[str] = msgspec.field(default_factory=list)
    degrade_reason: str | None = None
    payload_size_hint: int | None = None
    dropped_fields: list[str] | None = None
    truncated_fields: list[str] | None = None


class PythonEnrichmentPayload(CqOutputStruct, frozen=True):
    """Typed Python enrichment payload wrapper."""

    meta: EnrichmentMeta
    data: dict[str, object] = msgspec.field(default_factory=dict)


class RustEnrichmentPayload(CqOutputStruct, frozen=True):
    """Typed Rust enrichment payload wrapper."""

    meta: EnrichmentMeta
    data: dict[str, object] = msgspec.field(default_factory=dict)


class LanguageEnrichmentPort(Protocol):
    """Protocol for language-specific enrichment adapter operations."""

    language: QueryLanguage

    def payload_from_match(self, match: object) -> dict[str, object] | None:
        """Extract language-specific payload from an enriched match."""
        ...

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: dict[str, object],
    ) -> None:
        """Accumulate telemetry for one language payload."""
        ...

    def build_diagnostics(self, payload: Mapping[str, object]) -> list[dict[str, object]]:
        """Build diagnostics rows for semantic plane preview."""
        ...


__all__ = [
    "EnrichmentMeta",
    "EnrichmentStatus",
    "LanguageEnrichmentPort",
    "PythonEnrichmentPayload",
    "RustEnrichmentPayload",
]
