"""Typed artifact specifications linking canonical names to msgspec schemas.

This module defines ``ArtifactSpec`` constants for the top artifact names that
already have matching ``msgspec.Struct`` types in ``serde_artifacts``.  Each spec
is registered in the global ``ArtifactSpecRegistry`` at import time.

Import this module (or any of its constants) to ensure the specs are loaded into
the registry.
"""

from __future__ import annotations

from serde_artifacts import (
    DeltaStatsDecision,
    ExtractErrorsArtifact,
    IncrementalMetadataSnapshot,
    NormalizeOutputsArtifact,
    PlanScheduleArtifact,
    PlanSignalsArtifact,
    PlanValidationArtifact,
    RunManifest,
    RuntimeProfileSnapshot,
    SemanticValidationArtifact,
    ViewCacheArtifact,
)
from serde_schema_registry import ArtifactSpec, register_artifact_spec

VIEW_CACHE_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_cache_artifact_v1",
        description="Cache materialization artifact for view registration.",
        payload_type=ViewCacheArtifact,
    )
)

DELTA_STATS_DECISION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_stats_decision_v1",
        description="Resolved stats decision for a Delta write.",
        payload_type=DeltaStatsDecision,
    )
)

SEMANTIC_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_validation_v1",
        description="Semantic metadata validation artifact for a view.",
        payload_type=SemanticValidationArtifact,
    )
)

PLAN_SCHEDULE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_schedule_v1",
        description="Schedule artifact for deterministic plan scheduling.",
        payload_type=PlanScheduleArtifact,
    )
)

PLAN_VALIDATION_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_validation_v1",
        description="Validation artifact for plan evidence edges.",
        payload_type=PlanValidationArtifact,
    )
)

PLAN_SIGNALS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_signals_v1",
        description="Canonical plan-signal summary artifact.",
        payload_type=PlanSignalsArtifact,
    )
)

RUN_MANIFEST_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="run_manifest_v1",
        description="Canonical run manifest payload for deterministic outputs.",
        payload_type=RunManifest,
    )
)

NORMALIZE_OUTPUTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="normalize_outputs_v1",
        description="Normalize outputs summary artifact.",
        payload_type=NormalizeOutputsArtifact,
    )
)

EXTRACT_ERRORS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="extract_errors_v1",
        description="Extract error summary artifact.",
        payload_type=ExtractErrorsArtifact,
    )
)

RUNTIME_PROFILE_SNAPSHOT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="runtime_profile_snapshot_v1",
        description="Unified runtime profile snapshot for reproducibility.",
        payload_type=RuntimeProfileSnapshot,
    )
)

INCREMENTAL_METADATA_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="incremental_metadata_v1",
        description="Snapshot payload for incremental runtime metadata.",
        payload_type=IncrementalMetadataSnapshot,
    )
)

__all__ = [
    "DELTA_STATS_DECISION_SPEC",
    "EXTRACT_ERRORS_SPEC",
    "INCREMENTAL_METADATA_SPEC",
    "NORMALIZE_OUTPUTS_SPEC",
    "PLAN_SCHEDULE_SPEC",
    "PLAN_SIGNALS_SPEC",
    "PLAN_VALIDATION_SPEC",
    "RUNTIME_PROFILE_SNAPSHOT_SPEC",
    "RUN_MANIFEST_SPEC",
    "SEMANTIC_VALIDATION_SPEC",
    "VIEW_CACHE_ARTIFACT_SPEC",
]
