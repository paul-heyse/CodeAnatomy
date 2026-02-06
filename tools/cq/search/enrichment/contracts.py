"""Typed enrichment contracts shared across Python and Rust pipelines."""

from __future__ import annotations

import msgspec

EnrichmentStatus = str


class EnrichmentMeta(msgspec.Struct):
    """Common enrichment metadata."""

    language: str
    enrichment_status: EnrichmentStatus = "applied"
    enrichment_sources: list[str] = msgspec.field(default_factory=list)
    degrade_reason: str | None = None
    payload_size_hint: int | None = None
    dropped_fields: list[str] | None = None
    truncated_fields: list[str] | None = None


class PythonEnrichmentPayload(msgspec.Struct):
    """Typed Python enrichment payload wrapper."""

    meta: EnrichmentMeta
    data: dict[str, object] = msgspec.field(default_factory=dict)


class RustEnrichmentPayload(msgspec.Struct):
    """Typed Rust enrichment payload wrapper."""

    meta: EnrichmentMeta
    data: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = [
    "EnrichmentMeta",
    "EnrichmentStatus",
    "PythonEnrichmentPayload",
    "RustEnrichmentPayload",
]
