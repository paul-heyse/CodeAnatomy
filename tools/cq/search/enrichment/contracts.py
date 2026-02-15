"""Typed enrichment contracts shared across Python and Rust pipelines."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct

EnrichmentStatus = str


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


__all__ = [
    "EnrichmentMeta",
    "EnrichmentStatus",
    "PythonEnrichmentPayload",
    "RustEnrichmentPayload",
]
