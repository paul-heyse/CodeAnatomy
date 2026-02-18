"""Typed enrichment contracts shared across Python and Rust pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol

import msgspec

from tools.cq.core.structs import CqOutputStruct
from tools.cq.core.types import QueryLanguage
from tools.cq.search.enrichment.incremental_facts import IncrementalFacts
from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts
from tools.cq.search.enrichment.rust_facts import RustEnrichmentFacts

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
    facts: PythonEnrichmentFacts | None = None
    incremental: IncrementalFacts | None = None
    raw: dict[str, object] = msgspec.field(default_factory=dict)


class RustEnrichmentPayload(CqOutputStruct, frozen=True):
    """Typed Rust enrichment payload wrapper."""

    meta: EnrichmentMeta
    facts: RustEnrichmentFacts | None = None
    raw: dict[str, object] = msgspec.field(default_factory=dict)


type LanguageEnrichmentPayload = PythonEnrichmentPayload | RustEnrichmentPayload


class LanguageEnrichmentPort(Protocol):
    """Protocol for language-specific enrichment adapter operations."""

    language: QueryLanguage

    def payload_from_match(self, match: object) -> LanguageEnrichmentPayload | None:
        """Extract language-specific payload from an enriched match."""
        ...

    def accumulate_telemetry(
        self,
        lang_bucket: dict[str, object],
        payload: LanguageEnrichmentPayload,
    ) -> None:
        """Accumulate telemetry for one language payload."""
        ...

    def build_diagnostics(
        self,
        payload: Mapping[str, object] | LanguageEnrichmentPayload,
    ) -> list[dict[str, object]]:
        """Build diagnostics rows for semantic plane preview."""
        ...


__all__ = [
    "EnrichmentMeta",
    "EnrichmentStatus",
    "LanguageEnrichmentPayload",
    "LanguageEnrichmentPort",
    "PythonEnrichmentPayload",
    "RustEnrichmentPayload",
]
