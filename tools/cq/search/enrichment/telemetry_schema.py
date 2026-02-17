"""Typed telemetry schema for smart-search enrichment aggregation."""

from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStruct

_PYTHON_STAGE_NAMES: tuple[str, ...] = (
    "ast_grep",
    "python_ast",
    "import_detail",
    "python_resolution",
    "tree_sitter",
    "compile",
    "symtable",
    "dis",
    "inspect",
)


def _default_python_stages() -> dict[str, StageTelemetryBucket]:
    return {name: StageTelemetryBucket() for name in _PYTHON_STAGE_NAMES}


def _default_python_timings() -> dict[str, float]:
    return dict.fromkeys(_PYTHON_STAGE_NAMES, 0.0)


class StageTelemetryBucket(CqStruct, frozen=True, kw_only=True):
    """Per-stage status counters."""

    applied: int = 0
    degraded: int = 0
    skipped: int = 0


class RuntimeFlagsBucket(CqStruct, frozen=True, kw_only=True):
    """Runtime flag counters for one language."""

    did_exceed_match_limit: int = 0
    cancelled: int = 0


class PythonEnrichmentTelemetryV1(CqStruct, frozen=True, kw_only=True):
    """Python telemetry bucket contract."""

    applied: int = 0
    degraded: int = 0
    skipped: int = 0
    query_runtime: RuntimeFlagsBucket = msgspec.field(default_factory=RuntimeFlagsBucket)
    stages: dict[str, StageTelemetryBucket] = msgspec.field(default_factory=_default_python_stages)
    timings_ms: dict[str, float] = msgspec.field(default_factory=_default_python_timings)


class RustEnrichmentTelemetryV1(CqStruct, frozen=True, kw_only=True):
    """Rust telemetry bucket contract."""

    applied: int = 0
    degraded: int = 0
    skipped: int = 0
    query_runtime: RuntimeFlagsBucket = msgspec.field(default_factory=RuntimeFlagsBucket)
    query_pack_tags: int = 0
    distribution_profile_hits: int = 0
    drift_breaking_profile_hits: int = 0
    drift_removed_node_kinds: int = 0
    drift_removed_fields: int = 0


class EnrichmentTelemetryV1(CqStruct, frozen=True, kw_only=True):
    """Top-level typed enrichment telemetry envelope."""

    python: PythonEnrichmentTelemetryV1 = msgspec.field(default_factory=PythonEnrichmentTelemetryV1)
    rust: RustEnrichmentTelemetryV1 = msgspec.field(default_factory=RustEnrichmentTelemetryV1)


def default_enrichment_telemetry_mapping() -> dict[str, object]:
    """Return default telemetry mapping using deterministic struct conversion."""
    return msgspec.to_builtins(EnrichmentTelemetryV1(), order="deterministic")


__all__ = [
    "EnrichmentTelemetryV1",
    "PythonEnrichmentTelemetryV1",
    "RuntimeFlagsBucket",
    "RustEnrichmentTelemetryV1",
    "StageTelemetryBucket",
    "default_enrichment_telemetry_mapping",
]
