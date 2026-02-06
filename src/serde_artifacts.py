"""Canonical msgspec models for plan, view, and runtime artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

import msgspec

from core_types import (
    EventKindStr,
    HashStr,
    IdentifierStr,
    JsonValueLax,
    NonNegativeFloat,
    NonNegativeInt,
    RunIdStr,
    StatusStr,
)
from datafusion_engine.delta.protocol import (
    DeltaProtocolCompatibility,
    DeltaProtocolSnapshot,
)
from serde_msgspec import StructBaseCompat, StructBaseHotPath, export_json_schemas
from serde_msgspec_ext import (
    ExecutionPlanProtoBytes,
    LogicalPlanProtoBytes,
    OptimizedPlanProtoBytes,
    SubstraitBytes,
)
from utils.hashing import hash_msgpack_canonical

# JsonValueLax is required to model arbitrary JSON payloads in schema exports.

NonEmptyStr = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        title="Non-empty String",
        description="Non-empty string value.",
        examples=["value"],
    ),
]
HashValue = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        title="Hash Value",
        description="Deterministic hash value.",
        examples=["sha256:4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c"],
    ),
]
PlanSignature = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        title="Plan Signature",
        description="Deterministic plan signature string.",
        examples=["sig_01HZX4J3C8F8M2KQ"],
    ),
]
CachePolicyName = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        title="Cache Policy",
        description="Cache policy identifier.",
        examples=["prefer_cache"],
    ),
]
StatsPolicyName = Annotated[
    str,
    msgspec.Meta(
        min_length=1,
        title="Stats Policy",
        description="Delta stats policy identifier.",
        examples=["auto"],
    ),
]
PlanFingerprint = Annotated[
    HashStr,
    msgspec.Meta(
        min_length=32,
        max_length=128,
        title="Plan Fingerprint",
        description="Deterministic plan fingerprint for reproducibility.",
        examples=["4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c"],
    ),
]
PlanIdentityHash = Annotated[
    HashStr,
    msgspec.Meta(
        min_length=32,
        max_length=128,
        title="Plan Identity Hash",
        description="Deterministic plan identity hash for artifact rows.",
        examples=["9b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c"],
    ),
]
ProfileName = Annotated[
    IdentifierStr,
    msgspec.Meta(
        min_length=1,
        max_length=128,
        title="Profile Name",
        description="Runtime profile name.",
        examples=["default", "prod_primary"],
    ),
]
DatasetName = Annotated[
    IdentifierStr,
    msgspec.Meta(
        min_length=1,
        max_length=128,
        title="Dataset Name",
        description="Dataset identifier.",
        examples=["cpg_nodes", "analytics.events_v2"],
    ),
]
ViewName = Annotated[
    IdentifierStr,
    msgspec.Meta(
        min_length=1,
        max_length=128,
        title="View Name",
        description="View name.",
        examples=["example_view", "analytics.view_summary"],
    ),
]
RunId = Annotated[
    RunIdStr,
    msgspec.Meta(
        min_length=8,
        max_length=64,
        title="Run Id",
        description="Unique identifier for a pipeline run.",
        examples=["run_01HZX4J3C8F8M2KQ"],
    ),
]
EventKind = Annotated[
    EventKindStr,
    msgspec.Meta(
        min_length=1,
        max_length=64,
        title="Event Kind",
        description="Artifact event kind identifier.",
        examples=["plan", "execution", "write"],
    ),
]
ArtifactStatus = Annotated[
    StatusStr,
    msgspec.Meta(
        min_length=1,
        max_length=32,
        title="Artifact Status",
        description="Artifact status value.",
        examples=["ok", "error"],
    ),
]


class PlanArtifacts(StructBaseCompat, frozen=True):
    """Serializable plan artifacts captured during planning."""

    explain_tree_rows: tuple[dict[str, JsonValueLax], ...] | None
    explain_verbose_rows: tuple[dict[str, JsonValueLax], ...] | None
    explain_analyze_duration_ms: NonNegativeFloat | None
    explain_analyze_output_rows: NonNegativeInt | None
    df_settings: dict[str, str]
    planning_env_snapshot: dict[str, JsonValueLax]
    planning_env_hash: HashValue
    rulepack_snapshot: dict[str, JsonValueLax] | None
    rulepack_hash: HashValue | None
    information_schema_snapshot: dict[str, JsonValueLax]
    information_schema_hash: HashValue
    substrait_validation: dict[str, JsonValueLax] | None
    logical_plan_proto: LogicalPlanProtoBytes | None
    optimized_plan_proto: OptimizedPlanProtoBytes | None
    execution_plan_proto: ExecutionPlanProtoBytes | None
    udf_snapshot_hash: HashValue
    function_registry_hash: HashValue
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    udf_snapshot: dict[str, JsonValueLax]
    udf_planner_snapshot: dict[str, JsonValueLax] | None


class PlanProtoStatus(StructBaseCompat, frozen=True):
    """Status payload for plan proto serialization."""

    enabled: bool
    installed: bool | None
    reason: str | None = None


class DeltaStatsDecision(StructBaseCompat, frozen=True):
    """Resolved stats decision for a Delta write."""

    dataset_name: DatasetName
    stats_policy: StatsPolicyName
    stats_columns: tuple[str, ...] | None
    lineage_columns: tuple[str, ...]
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    stats_max_columns: NonNegativeInt | None = None


class ViewCacheArtifact(StructBaseHotPath, frozen=True):
    """Cache materialization artifact for view registration."""

    view_name: ViewName
    cache_policy: CachePolicyName
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
    protocol: DeltaProtocolSnapshot | None = None
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
    schema: dict[str, JsonValueLax] | None = None


class PlanArtifactRow(StructBaseCompat, frozen=True):
    """Serializable plan artifact row persisted to the Delta store."""

    event_time_unix_ms: NonNegativeInt
    profile_name: ProfileName | None
    event_kind: EventKind
    view_name: ViewName
    plan_fingerprint: PlanFingerprint
    plan_identity_hash: PlanIdentityHash
    udf_snapshot_hash: HashValue
    function_registry_hash: HashValue
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    delta_inputs_msgpack: bytes
    df_settings: dict[str, str]
    planning_env_msgpack: bytes
    planning_env_hash: HashValue
    rulepack_msgpack: bytes | None
    rulepack_hash: HashValue | None
    information_schema_msgpack: bytes
    information_schema_hash: HashValue
    substrait_msgpack: bytes
    logical_plan_proto_msgpack: bytes | None
    optimized_plan_proto_msgpack: bytes | None
    execution_plan_proto_msgpack: bytes | None
    explain_tree_rows_msgpack: bytes | None
    explain_verbose_rows_msgpack: bytes | None
    explain_analyze_duration_ms: NonNegativeFloat | None
    explain_analyze_output_rows: NonNegativeInt | None
    substrait_validation_msgpack: bytes | None
    lineage_msgpack: bytes
    scan_units_msgpack: bytes
    scan_keys: tuple[str, ...]
    plan_details_msgpack: bytes
    udf_snapshot_msgpack: bytes
    udf_planner_snapshot_msgpack: bytes | None
    udf_compatibility_ok: bool
    udf_compatibility_detail_msgpack: bytes
    execution_duration_ms: NonNegativeFloat | None = None
    execution_status: str | None = None
    execution_error: str | None = None

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for Arrow/Delta ingestion.

        Returns:
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

    event_time_unix_ms: NonNegativeInt
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
    commit_run_id: RunId | None
    delta_write_policy_msgpack: msgspec.Raw
    delta_schema_policy_msgpack: msgspec.Raw
    partition_by: tuple[str, ...]
    table_properties: dict[str, str]
    commit_metadata: dict[str, str]
    delta_stats_decision_msgpack: msgspec.Raw
    duration_ms: NonNegativeFloat | None
    row_count: NonNegativeInt | None
    status: str | None
    error: str | None

    def to_row(self) -> dict[str, object]:
        """Return a row mapping for Arrow/Delta ingestion.

        Returns:
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
            "delta_write_policy_msgpack": bytes(self.delta_write_policy_msgpack),
            "delta_schema_policy_msgpack": bytes(self.delta_schema_policy_msgpack),
            "partition_by": list(self.partition_by),
            "table_properties": dict(self.table_properties),
            "commit_metadata": dict(self.commit_metadata),
            "delta_stats_decision_msgpack": bytes(self.delta_stats_decision_msgpack),
            "duration_ms": self.duration_ms,
            "row_count": self.row_count,
            "status": self.status,
            "error": self.error,
        }


class SubstraitPayload(StructBaseCompat, frozen=True):
    """Named wrapper for Substrait bytes payloads."""

    payload: SubstraitBytes
    fingerprint: HashValue


class ViewArtifactPayload(StructBaseCompat, frozen=True):
    """Serializable view artifact payload."""

    name: ViewName
    plan_fingerprint: PlanFingerprint
    plan_task_signature: PlanSignature
    schema: dict[str, JsonValueLax]
    schema_describe: tuple[dict[str, JsonValueLax], ...]
    schema_provenance: dict[str, JsonValueLax]
    required_udfs: tuple[str, ...]
    referenced_tables: tuple[str, ...]


class RuntimeProfileSnapshot(StructBaseCompat, frozen=True):
    """Unified runtime profile snapshot for reproducibility."""

    version: NonNegativeInt
    name: ProfileName
    determinism_tier: NonEmptyStr
    datafusion_settings_hash: HashValue
    datafusion_settings: dict[str, str]
    telemetry_payload: dict[str, JsonValueLax]
    profile_hash: HashValue

    def payload(self) -> dict[str, object]:
        """Return the snapshot payload for serialization.

        Returns:
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

    datafusion_settings_hash: HashValue
    runtime_profile_hash: HashValue
    runtime_profile: RuntimeProfileSnapshot


class RunManifest(StructBaseHotPath, frozen=True):
    """Canonical run manifest payload for deterministic outputs."""

    run_id: RunId
    status: ArtifactStatus
    event_time_unix_ms: NonNegativeInt
    plan_signature: PlanSignature | None
    plan_fingerprints: dict[str, PlanFingerprint]
    delta_inputs: tuple[dict[str, JsonValueLax], ...]
    outputs: tuple[dict[str, JsonValueLax], ...]
    runtime_profile_name: ProfileName | None
    runtime_profile_hash: HashValue | None
    determinism_tier: NonEmptyStr | None
    output_dir: str | None
    artifact_ids: dict[str, str] | None = None
    cache_path: str | None = None
    cache_log_glob: str | None = None
    cache_policy_profile: str | None = None
    cache_log_enabled: bool | None = None
    materialized_outputs: tuple[str, ...] | None = None


class NormalizeOutputsArtifact(StructBaseCompat, frozen=True):
    """Normalize outputs summary artifact."""

    event_time_unix_ms: NonNegativeInt
    run_id: RunId
    output_dir: str
    outputs: tuple[str, ...]
    row_count: NonNegativeInt


class ExtractErrorsArtifact(StructBaseCompat, frozen=True):
    """Extract error summary artifact."""

    event_time_unix_ms: NonNegativeInt
    run_id: RunId
    output_dir: str
    errors: tuple[str, ...]
    error_count: NonNegativeInt


class ArtifactEnvelopeBase(StructBaseHotPath, frozen=True, tag=True, tag_field="kind"):
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
    plan_signature: PlanSignature
    reduced_plan_signature: PlanSignature
    task_count: NonNegativeInt
    ordered_tasks: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    critical_path_tasks: tuple[str, ...]
    critical_path_length_weighted: NonNegativeFloat | None
    task_costs: dict[str, NonNegativeFloat]
    bottom_level_costs: dict[str, NonNegativeFloat]
    slack_by_task: dict[str, NonNegativeFloat] | None = None
    task_centrality: dict[str, NonNegativeFloat] | None = None
    task_dominators: dict[str, str | None] | None = None
    bridge_edges: tuple[tuple[str, str], ...] = ()
    articulation_tasks: tuple[str, ...] = ()


class PlanScheduleEnvelope(ArtifactEnvelopeBase, tag="plan_schedule", frozen=True):
    """Envelope for plan schedule artifacts."""

    payload: PlanScheduleArtifact


class PlanValidationArtifact(StructBaseCompat, frozen=True):
    """Validation artifact for plan evidence edges."""

    run_id: RunId
    plan_signature: PlanSignature
    reduced_plan_signature: PlanSignature
    total_tasks: NonNegativeInt
    valid_tasks: NonNegativeInt
    invalid_tasks: NonNegativeInt
    total_edges: NonNegativeInt
    valid_edges: NonNegativeInt
    invalid_edges: NonNegativeInt
    task_results: tuple[dict[str, JsonValueLax], ...]


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

    Returns:
    -------
    str
        SHA-256 hash of the msgpack payload.
    """
    return hash_msgpack_canonical(envelope)


def artifact_schema_types() -> tuple[type[msgspec.Struct], ...]:
    """Return msgspec struct types exported as schema contracts.

    Returns:
    -------
    tuple[type[msgspec.Struct], ...]
        msgspec struct types for schema export.
    """
    types: list[type[msgspec.Struct]] = []
    for name in __all__:
        value = globals().get(name)
        if isinstance(value, type) and issubclass(value, msgspec.Struct):
            types.append(value)
    return tuple(types)


def export_artifact_schemas(output_dir: Path) -> tuple[Path, ...]:
    """Export JSON Schema payloads for artifact msgspec structs.

    Parameters
    ----------
    output_dir
        Directory to write schema files into.

    Returns:
    -------
    tuple[Path, ...]
        Paths to the generated schema files.
    """
    return export_json_schemas(artifact_schema_types(), output_dir=output_dir)


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
    "artifact_schema_types",
    "export_artifact_schemas",
]
