"""Canonical msgspec models for plan, view, and runtime artifacts."""

from __future__ import annotations

import hashlib
from typing import Annotated, Any, Literal

import msgspec

from datafusion_engine.delta_protocol import (
    DeltaFeatureGate,
    DeltaProtocolCompatibility,
    DeltaProtocolSnapshot,
)
from serde_msgspec import StructBaseCompat, dumps_msgpack
from serde_msgspec_ext import (
    ExecutionPlanProtoBytes,
    LogicalPlanProtoBytes,
    OptimizedPlanProtoBytes,
    SubstraitBytes,
)

# Any is required to model arbitrary JSON payloads in schema exports.
type JsonValue = Any

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]
NonNegFloat = Annotated[float, msgspec.Meta(ge=0)]
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]
PlanFingerprint = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Deterministic plan fingerprint for reproducibility."),
]
PlanIdentityHash = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Deterministic plan identity hash for artifact rows."),
]
ProfileName = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Runtime profile name."),
]
DatasetName = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Dataset identifier."),
]
ViewName = Annotated[
    str,
    msgspec.Meta(min_length=1, description="View name."),
]
RunId = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Unique run identifier."),
]
EventKind = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Artifact event kind identifier."),
]
ArtifactStatus = Annotated[
    str,
    msgspec.Meta(min_length=1, description="Artifact status value."),
]


class PlanArtifacts(StructBaseCompat, frozen=True):
    """Serializable plan artifacts captured during planning."""

    explain_tree_rows: tuple[dict[str, JsonValue], ...] | None
    explain_verbose_rows: tuple[dict[str, JsonValue], ...] | None
    explain_analyze_duration_ms: NonNegFloat | None
    explain_analyze_output_rows: NonNegInt | None
    df_settings: dict[str, str]
    planning_env_snapshot: dict[str, JsonValue]
    planning_env_hash: str
    rulepack_snapshot: dict[str, JsonValue] | None
    rulepack_hash: str | None
    information_schema_snapshot: dict[str, JsonValue]
    information_schema_hash: str
    substrait_validation: dict[str, JsonValue] | None
    logical_plan_proto: LogicalPlanProtoBytes | None
    optimized_plan_proto: OptimizedPlanProtoBytes | None
    execution_plan_proto: ExecutionPlanProtoBytes | None
    udf_snapshot_hash: str
    function_registry_hash: str
    function_registry_snapshot: dict[str, JsonValue]
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    udf_snapshot: dict[str, JsonValue]
    udf_planner_snapshot: dict[str, JsonValue] | None


class PlanProtoStatus(StructBaseCompat, frozen=True):
    """Status payload for plan proto serialization."""

    enabled: bool
    installed: bool | None
    reason: str | None = None


class DeltaStatsDecision(StructBaseCompat, frozen=True):
    """Resolved stats decision for a Delta write."""

    dataset_name: DatasetName
    stats_policy: NonEmptyStr
    stats_columns: tuple[str, ...] | None
    lineage_columns: tuple[str, ...]
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    stats_max_columns: NonNegInt | None = None


class ViewCacheArtifact(StructBaseCompat, frozen=True):
    """Cache materialization artifact for view registration."""

    view_name: ViewName
    cache_policy: NonEmptyStr
    cache_path: str | None
    plan_fingerprint: PlanFingerprint | None
    status: ArtifactStatus
    hit: bool | None = None


class SemanticValidationEntry(StructBaseCompat, frozen=True):
    """Validation entry for semantic metadata enforcement."""

    column_name: NonEmptyStr
    expected: NonEmptyStr
    actual: str | None
    status: Literal["ok", "missing", "mismatch"]


class SemanticValidationArtifact(StructBaseCompat, frozen=True):
    """Semantic metadata validation artifact for a view."""

    view_name: ViewName
    status: Literal["ok", "error"]
    entries: tuple[SemanticValidationEntry, ...]
    errors: tuple[str, ...] = ()


class DeltaInputPin(StructBaseCompat, frozen=True):
    """Pinned Delta version information for a scan input."""

    dataset_name: DatasetName
    version: int | None
    timestamp: str | None
    feature_gate: DeltaFeatureGate | None = None
    protocol: DeltaProtocolSnapshot | None = None
    storage_options_hash: str | None = None
    delta_scan_config: DeltaScanConfigSnapshot | None = None
    delta_scan_config_hash: str | None = None
    datafusion_provider: str | None = None
    protocol_compatible: bool | None = None
    protocol_compatibility: DeltaProtocolCompatibility | None = None


class DeltaScanConfigSnapshot(StructBaseCompat, frozen=True):
    """Snapshot of Delta scan configuration for artifacts."""

    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool = False
    schema: dict[str, JsonValue] | None = None


class PlanArtifactRow(StructBaseCompat, frozen=True):
    """Serializable plan artifact row persisted to the Delta store."""

    event_time_unix_ms: NonNegInt
    profile_name: ProfileName | None
    event_kind: EventKind
    view_name: ViewName
    plan_fingerprint: PlanFingerprint
    plan_identity_hash: PlanIdentityHash
    udf_snapshot_hash: str
    function_registry_hash: str
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    delta_inputs_msgpack: bytes
    df_settings: dict[str, str]
    planning_env_msgpack: bytes
    planning_env_hash: str
    rulepack_msgpack: bytes | None
    rulepack_hash: str | None
    information_schema_msgpack: bytes
    information_schema_hash: str
    substrait_msgpack: bytes | None
    logical_plan_proto_msgpack: bytes | None
    optimized_plan_proto_msgpack: bytes | None
    execution_plan_proto_msgpack: bytes | None
    explain_tree_rows_msgpack: bytes | None
    explain_verbose_rows_msgpack: bytes | None
    explain_analyze_duration_ms: NonNegFloat | None
    explain_analyze_output_rows: NonNegInt | None
    substrait_validation_msgpack: bytes | None
    lineage_msgpack: bytes
    scan_units_msgpack: bytes
    scan_keys: tuple[str, ...]
    plan_details_msgpack: bytes
    function_registry_snapshot_msgpack: bytes
    udf_snapshot_msgpack: bytes
    udf_planner_snapshot_msgpack: bytes | None
    udf_compatibility_ok: bool
    udf_compatibility_detail_msgpack: bytes
    execution_duration_ms: NonNegFloat | None = None
    execution_status: str | None = None
    execution_error: str | None = None

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for Arrow/Delta ingestion.

        Returns
        -------
        dict[str, object]
            Row mapping for ingestion.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms,
            "profile_name": self.profile_name,
            "event_kind": self.event_kind,
            "view_name": self.view_name,
            "plan_fingerprint": self.plan_fingerprint,
            "plan_identity_hash": self.plan_identity_hash,
            "udf_snapshot_hash": self.udf_snapshot_hash,
            "function_registry_hash": self.function_registry_hash,
            "required_udfs": list(self.required_udfs),
            "required_rewrite_tags": list(self.required_rewrite_tags),
            "domain_planner_names": list(self.domain_planner_names),
            "delta_inputs_msgpack": self.delta_inputs_msgpack,
            "df_settings": dict(self.df_settings),
            "planning_env_msgpack": self.planning_env_msgpack,
            "planning_env_hash": self.planning_env_hash,
            "rulepack_msgpack": self.rulepack_msgpack,
            "rulepack_hash": self.rulepack_hash,
            "information_schema_msgpack": self.information_schema_msgpack,
            "information_schema_hash": self.information_schema_hash,
            "substrait_msgpack": self.substrait_msgpack,
            "logical_plan_proto_msgpack": self.logical_plan_proto_msgpack,
            "optimized_plan_proto_msgpack": self.optimized_plan_proto_msgpack,
            "execution_plan_proto_msgpack": self.execution_plan_proto_msgpack,
            "explain_tree_rows_msgpack": self.explain_tree_rows_msgpack,
            "explain_verbose_rows_msgpack": self.explain_verbose_rows_msgpack,
            "explain_analyze_duration_ms": self.explain_analyze_duration_ms,
            "explain_analyze_output_rows": self.explain_analyze_output_rows,
            "substrait_validation_msgpack": self.substrait_validation_msgpack,
            "lineage_msgpack": self.lineage_msgpack,
            "scan_units_msgpack": self.scan_units_msgpack,
            "scan_keys": list(self.scan_keys),
            "plan_details_msgpack": self.plan_details_msgpack,
            "function_registry_snapshot_msgpack": self.function_registry_snapshot_msgpack,
            "udf_snapshot_msgpack": self.udf_snapshot_msgpack,
            "udf_planner_snapshot_msgpack": self.udf_planner_snapshot_msgpack,
            "udf_compatibility_ok": self.udf_compatibility_ok,
            "udf_compatibility_detail_msgpack": self.udf_compatibility_detail_msgpack,
            "execution_duration_ms": self.execution_duration_ms,
            "execution_status": self.execution_status,
            "execution_error": self.execution_error,
        }


class WriteArtifactRow(StructBaseCompat, frozen=True):
    """Serializable write artifact row persisted to the Delta store."""

    event_time_unix_ms: NonNegInt
    profile_name: ProfileName | None
    event_kind: EventKind
    destination: str
    format: str
    mode: str
    method: str
    table_uri: str
    delta_version: int | None
    commit_app_id: str | None
    commit_version: int | None
    commit_run_id: str | None
    delta_write_policy_msgpack: bytes
    delta_schema_policy_msgpack: bytes
    partition_by: tuple[str, ...]
    table_properties: dict[str, str]
    commit_metadata: dict[str, str]
    delta_stats_decision_msgpack: bytes
    duration_ms: NonNegFloat | None
    row_count: NonNegInt | None
    status: str | None
    error: str | None

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for Arrow/Delta ingestion.

        Returns
        -------
        dict[str, object]
            Row mapping for ingestion.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms,
            "profile_name": self.profile_name,
            "event_kind": self.event_kind,
            "destination": self.destination,
            "format": self.format,
            "mode": self.mode,
            "method": self.method,
            "table_uri": self.table_uri,
            "delta_version": self.delta_version,
            "commit_app_id": self.commit_app_id,
            "commit_version": self.commit_version,
            "commit_run_id": self.commit_run_id,
            "delta_write_policy_msgpack": self.delta_write_policy_msgpack,
            "delta_schema_policy_msgpack": self.delta_schema_policy_msgpack,
            "partition_by": list(self.partition_by),
            "table_properties": dict(self.table_properties),
            "commit_metadata": dict(self.commit_metadata),
            "delta_stats_decision_msgpack": self.delta_stats_decision_msgpack,
            "duration_ms": self.duration_ms,
            "row_count": self.row_count,
            "status": self.status,
            "error": self.error,
        }


class SubstraitPayload(StructBaseCompat, frozen=True):
    """Named wrapper for Substrait bytes payloads."""

    payload: SubstraitBytes
    fingerprint: str


class ViewArtifactPayload(StructBaseCompat, frozen=True):
    """Serializable view artifact payload."""

    name: ViewName
    plan_fingerprint: PlanFingerprint
    plan_task_signature: str
    schema: dict[str, JsonValue]
    schema_describe: tuple[dict[str, JsonValue], ...]
    schema_provenance: dict[str, JsonValue]
    required_udfs: tuple[str, ...]
    referenced_tables: tuple[str, ...]


class RuntimeProfileSnapshot(StructBaseCompat, frozen=True):
    """Unified runtime profile snapshot for reproducibility."""

    version: NonNegInt
    name: ProfileName
    determinism_tier: str
    datafusion_settings_hash: str
    datafusion_settings: dict[str, str]
    telemetry_payload: dict[str, JsonValue]
    profile_hash: str

    def payload(self) -> dict[str, object]:
        """Return the snapshot payload for serialization.

        Returns
        -------
        dict[str, object]
            Snapshot payload mapping.
        """
        return {
            "version": self.version,
            "name": self.name,
            "determinism_tier": self.determinism_tier,
            "datafusion_settings_hash": self.datafusion_settings_hash,
            "datafusion_settings": self.datafusion_settings,
            "telemetry_payload": self.telemetry_payload,
            "profile_hash": self.profile_hash,
        }


class IncrementalMetadataSnapshot(StructBaseCompat, frozen=True):
    """Snapshot payload for incremental runtime metadata."""

    datafusion_settings_hash: str
    runtime_profile_hash: str
    runtime_profile: RuntimeProfileSnapshot


class RunManifest(StructBaseCompat, frozen=True):
    """Canonical run manifest payload for deterministic outputs."""

    run_id: RunId
    status: ArtifactStatus
    event_time_unix_ms: NonNegInt
    plan_signature: str | None
    plan_fingerprints: dict[str, str]
    delta_inputs: tuple[dict[str, JsonValue], ...]
    outputs: tuple[dict[str, JsonValue], ...]
    runtime_profile_name: ProfileName | None
    runtime_profile_hash: str | None
    determinism_tier: str | None
    output_dir: str | None
    artifact_ids: dict[str, str] | None = None
    cache_path: str | None = None
    cache_log_glob: str | None = None
    cache_policy_profile: str | None = None
    cache_log_enabled: bool | None = None
    materialized_outputs: tuple[str, ...] | None = None


class NormalizeOutputsArtifact(StructBaseCompat, frozen=True):
    """Normalize outputs summary artifact."""

    event_time_unix_ms: NonNegInt
    run_id: RunId
    output_dir: str
    outputs: tuple[str, ...]
    row_count: NonNegInt


class ExtractErrorsArtifact(StructBaseCompat, frozen=True):
    """Extract error summary artifact."""

    event_time_unix_ms: NonNegInt
    run_id: RunId
    output_dir: str
    errors: tuple[str, ...]
    error_count: NonNegInt


class ArtifactEnvelopeBase(StructBaseCompat, frozen=True, tag=True, tag_field="kind"):
    """Tagged envelope for artifact payload streams."""


class DeltaStatsDecisionEnvelope(ArtifactEnvelopeBase, tag="delta_stats_decision", frozen=True):
    """Envelope for Delta stats decision artifacts."""

    payload: DeltaStatsDecision


class ViewCacheArtifactEnvelope(ArtifactEnvelopeBase, tag="view_cache_artifact", frozen=True):
    """Envelope for view cache artifacts."""

    payload: ViewCacheArtifact


class SemanticValidationArtifactEnvelope(
    ArtifactEnvelopeBase, tag="semantic_validation", frozen=True
):
    """Envelope for semantic validation artifacts."""

    payload: SemanticValidationArtifact


class PlanScheduleArtifact(StructBaseCompat, frozen=True):
    """Schedule artifact for deterministic plan scheduling."""

    run_id: RunId
    plan_signature: str
    reduced_plan_signature: str
    task_count: NonNegInt
    ordered_tasks: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    critical_path_tasks: tuple[str, ...]
    critical_path_length_weighted: NonNegFloat | None
    task_costs: dict[str, NonNegFloat]
    bottom_level_costs: dict[str, NonNegFloat]
    slack_by_task: dict[str, NonNegFloat] | None = None
    task_centrality: dict[str, NonNegFloat] | None = None
    task_dominators: dict[str, str | None] | None = None
    bridge_edges: tuple[tuple[str, str], ...] = ()
    articulation_tasks: tuple[str, ...] = ()


class PlanScheduleEnvelope(ArtifactEnvelopeBase, tag="plan_schedule", frozen=True):
    """Envelope for plan schedule artifacts."""

    payload: PlanScheduleArtifact


class PlanValidationArtifact(StructBaseCompat, frozen=True):
    """Validation artifact for plan evidence edges."""

    run_id: RunId
    plan_signature: str
    reduced_plan_signature: str
    total_tasks: NonNegInt
    valid_tasks: NonNegInt
    invalid_tasks: NonNegInt
    total_edges: NonNegInt
    valid_edges: NonNegInt
    invalid_edges: NonNegInt
    task_results: tuple[dict[str, JsonValue], ...]


class PlanValidationEnvelope(ArtifactEnvelopeBase, tag="plan_validation", frozen=True):
    """Envelope for plan validation artifacts."""

    payload: PlanValidationArtifact


class RunManifestEnvelope(ArtifactEnvelopeBase, tag="run_manifest", frozen=True):
    """Envelope for run manifest artifacts."""

    payload: RunManifest


def artifact_envelope_id(envelope: ArtifactEnvelopeBase) -> str:
    """Return a deterministic hash identifier for an artifact envelope.

    Parameters
    ----------
    envelope
        Artifact envelope to hash.

    Returns
    -------
    str
        SHA-256 hash of the msgpack payload.
    """
    return hashlib.sha256(dumps_msgpack(envelope)).hexdigest()


__all__ = [
    "ArtifactEnvelopeBase",
    "DeltaInputPin",
    "DeltaScanConfigSnapshot",
    "DeltaStatsDecision",
    "DeltaStatsDecisionEnvelope",
    "ExecutionPlanProtoBytes",
    "ExtractErrorsArtifact",
    "IncrementalMetadataSnapshot",
    "LogicalPlanProtoBytes",
    "NormalizeOutputsArtifact",
    "OptimizedPlanProtoBytes",
    "PlanArtifactRow",
    "PlanArtifacts",
    "PlanProtoStatus",
    "PlanScheduleArtifact",
    "PlanScheduleEnvelope",
    "PlanValidationArtifact",
    "PlanValidationEnvelope",
    "RunManifest",
    "RunManifestEnvelope",
    "RuntimeProfileSnapshot",
    "SemanticValidationArtifact",
    "SemanticValidationArtifactEnvelope",
    "SemanticValidationEntry",
    "SubstraitBytes",
    "SubstraitPayload",
    "ViewArtifactPayload",
    "ViewCacheArtifact",
    "ViewCacheArtifactEnvelope",
    "WriteArtifactRow",
    "artifact_envelope_id",
]
