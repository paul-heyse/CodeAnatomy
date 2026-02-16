"""Runtime contracts for identifier normalization and telemetry enrichment."""

from __future__ import annotations

from enum import StrEnum

import msgspec


class IdentifierNormalizationMode(StrEnum):
    """Identifier normalization behavior for SQL parser settings."""

    RAW = "raw"
    SQL_SAFE = "sql_safe"
    STRICT = "strict"


class TelemetryEnrichmentPolicy(msgspec.Struct, frozen=True):
    """Policy controlling telemetry payload enrichment behavior."""

    include_query_text: bool = False
    include_plan_hash: bool = True
    include_profile_name: bool = True


__all__ = ["IdentifierNormalizationMode", "TelemetryEnrichmentPolicy"]
