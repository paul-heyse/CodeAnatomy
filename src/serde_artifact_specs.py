"""Typed artifact specifications linking canonical names to msgspec schemas.

This module defines ``ArtifactSpec`` constants for the top artifact names that
already have matching ``msgspec.Struct`` types in ``serde_artifacts``.  Each spec
is registered in the global ``ArtifactSpecRegistry`` at import time.

Specs whose callsites still pass raw dict payloads use ``payload_type=None``
(unvalidated) until a matching ``msgspec.Struct`` is introduced.

Import this module (or any of its constants) to ensure the specs are loaded into
the registry.
"""

from __future__ import annotations

from serde_artifacts import (
    DeltaProtocolArtifact,
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
    ViewArtifactPayload,
    ViewCacheArtifact,
    WriteArtifactRow,
)
from serde_schema_registry import ArtifactSpec, register_artifact_spec

# ---------------------------------------------------------------------------
# Typed specs (with matching msgspec Struct payload types)
# ---------------------------------------------------------------------------

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

DELTA_PROTOCOL_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_protocol_compatibility_v1",
        description="Delta protocol compatibility diagnostic artifact.",
        payload_type=DeltaProtocolArtifact,
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

WRITE_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="write_artifact_v2",
        description="Write artifact row persisted to the Delta store.",
        payload_type=WriteArtifactRow,
    )
)

DATAFUSION_VIEW_ARTIFACTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_view_artifacts_v4",
        description="Deterministic view artifact payload with plan fingerprints.",
        payload_type=ViewArtifactPayload,
    )
)

# ---------------------------------------------------------------------------
# Untyped specs (payload_type=None, high-blast-radius artifact names)
# ---------------------------------------------------------------------------

DELTA_MAINTENANCE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="delta_maintenance_v1",
        description="Delta table maintenance diagnostics (optimize/vacuum/checkpoint).",
    )
)

PLAN_EXECUTE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="plan_execute_v1",
        description="Plan execution event diagnostics payload.",
    )
)

SCHEMA_CONTRACT_VIOLATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="schema_contract_violations_v1",
        description="Schema contract violations detected during view validation.",
    )
)

VIEW_CONTRACT_VIOLATIONS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_contract_violations_v1",
        description="View-level schema contract violation diagnostics.",
    )
)

VIEW_UDF_PARITY_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_udf_parity_v1",
        description="View/UDF parity diagnostics describing required and missing UDFs.",
    )
)

VIEW_FINGERPRINTS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="view_fingerprints_v1",
        description="Policy-aware view fingerprint diagnostics payload.",
    )
)

HAMILTON_CACHE_LINEAGE_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="hamilton_cache_lineage_v2",
        description="Hamilton cache lineage summary with per-node facts.",
    )
)

DATASET_READINESS_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="dataset_readiness_v1",
        description="Dataset readiness check diagnostics for heartbeat blockers.",
    )
)

SEMANTIC_QUALITY_ARTIFACT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="semantic_quality_artifact_v1",
        description="Semantic quality artifact summary payload.",
    )
)

DATAFUSION_DELTA_COMMIT_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_delta_commit_v1",
        description="Delta commit diagnostics for write and mutation operations.",
    )
)

DATAFUSION_RUN_STARTED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_run_started_v1",
        description="DataFusion pipeline run start event.",
    )
)

DATAFUSION_RUN_FINISHED_SPEC = register_artifact_spec(
    ArtifactSpec(
        canonical_name="datafusion_run_finished_v1",
        description="DataFusion pipeline run completion event.",
    )
)

__all__ = [
    "DATAFUSION_DELTA_COMMIT_SPEC",
    "DATAFUSION_RUN_FINISHED_SPEC",
    "DATAFUSION_RUN_STARTED_SPEC",
    "DATAFUSION_VIEW_ARTIFACTS_SPEC",
    "DATASET_READINESS_SPEC",
    "DELTA_MAINTENANCE_SPEC",
    "DELTA_PROTOCOL_ARTIFACT_SPEC",
    "DELTA_STATS_DECISION_SPEC",
    "EXTRACT_ERRORS_SPEC",
    "HAMILTON_CACHE_LINEAGE_SPEC",
    "INCREMENTAL_METADATA_SPEC",
    "NORMALIZE_OUTPUTS_SPEC",
    "PLAN_EXECUTE_SPEC",
    "PLAN_SCHEDULE_SPEC",
    "PLAN_SIGNALS_SPEC",
    "PLAN_VALIDATION_SPEC",
    "RUNTIME_PROFILE_SNAPSHOT_SPEC",
    "RUN_MANIFEST_SPEC",
    "SCHEMA_CONTRACT_VIOLATIONS_SPEC",
    "SEMANTIC_QUALITY_ARTIFACT_SPEC",
    "SEMANTIC_VALIDATION_SPEC",
    "VIEW_CACHE_ARTIFACT_SPEC",
    "VIEW_CONTRACT_VIOLATIONS_SPEC",
    "VIEW_FINGERPRINTS_SPEC",
    "VIEW_UDF_PARITY_SPEC",
    "WRITE_ARTIFACT_SPEC",
]
