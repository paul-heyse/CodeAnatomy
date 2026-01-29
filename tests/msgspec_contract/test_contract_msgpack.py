"""MessagePack contract tests for msgspec payloads."""

from __future__ import annotations

import datetime as dt

from serde_artifacts import (
    DeltaStatsDecision,
    DeltaStatsDecisionEnvelope,
    PlanArtifactRow,
    PlanProtoStatus,
    PlanScheduleArtifact,
    PlanScheduleEnvelope,
    PlanValidationArtifact,
    PlanValidationEnvelope,
    RunManifest,
    RunManifestEnvelope,
    SemanticValidationArtifact,
    SemanticValidationArtifactEnvelope,
    SemanticValidationEntry,
    ViewCacheArtifact,
    ViewCacheArtifactEnvelope,
)
from serde_msgspec import dumps_msgpack
from serde_msgspec_ext import LogicalPlanProtoBytes, SubstraitBytes
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy, ParquetWriterPolicy
from tests.msgspec_contract._support.codecs import decode_msgpack, encode_msgpack
from tests.msgspec_contract._support.goldens import GOLDENS_DIR, assert_bytes_snapshot_b64
from tests.msgspec_contract._support.models import Click, Envelope, User


def _sample_envelope() -> Envelope:
    return Envelope(
        user=User(
            id="user-2",
            name="Grace",
            created_at=dt.datetime(2025, 2, 2, 9, 0, 0, tzinfo=dt.UTC),
        ),
        event=Click(
            ts=dt.datetime(2025, 2, 2, 9, 5, 0, tzinfo=dt.UTC),
            page="/home",
        ),
        metadata={"source": "tests"},
    )


def test_msgpack_contract_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for the envelope contract."""
    envelope = _sample_envelope()
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=Envelope)
    assert decoded == envelope


def _sample_plan_artifact_row() -> PlanArtifactRow:
    substrait_payload = dumps_msgpack(SubstraitBytes(b"substrait"))
    logical_payload = dumps_msgpack(LogicalPlanProtoBytes(b"logical"))
    delta_inputs: tuple[dict[str, object], ...] = (
        {
            "dataset_name": "cpg_nodes",
            "version": 1,
            "timestamp": None,
        },
    )
    return PlanArtifactRow(
        event_time_unix_ms=1735689600000,
        profile_name="default",
        event_kind="plan",
        view_name="example_view",
        plan_fingerprint="fp-1",
        plan_identity_hash="pid-1",
        udf_snapshot_hash="udf-hash",
        function_registry_hash="registry-hash",
        required_udfs=("udf_a",),
        required_rewrite_tags=("rewrite_a",),
        domain_planner_names=("domain_a",),
        delta_inputs_msgpack=dumps_msgpack(delta_inputs),
        df_settings={"execution_mode": "test"},
        planning_env_msgpack=dumps_msgpack({"python_version": "3.13"}),
        planning_env_hash="env-hash",
        rulepack_msgpack=None,
        rulepack_hash=None,
        information_schema_msgpack=dumps_msgpack({"tables": []}),
        information_schema_hash="info-hash",
        substrait_msgpack=substrait_payload,
        logical_plan_proto_msgpack=logical_payload,
        optimized_plan_proto_msgpack=None,
        execution_plan_proto_msgpack=None,
        explain_tree_rows_msgpack=None,
        explain_verbose_rows_msgpack=None,
        explain_analyze_duration_ms=None,
        explain_analyze_output_rows=None,
        substrait_validation_msgpack=None,
        lineage_msgpack=dumps_msgpack({"nodes": []}),
        scan_units_msgpack=dumps_msgpack(({"name": "scan-1"},)),
        scan_keys=("scan_key_1",),
        plan_details_msgpack=dumps_msgpack({"mode": "test"}),
        function_registry_snapshot_msgpack=dumps_msgpack({"hash": "registry-hash"}),
        udf_snapshot_msgpack=dumps_msgpack({"hash": "udf-hash"}),
        udf_planner_snapshot_msgpack=None,
        udf_compatibility_ok=True,
        udf_compatibility_detail_msgpack=dumps_msgpack({"status": "ok"}),
        execution_duration_ms=None,
        execution_status=None,
        execution_error=None,
    )


def test_msgpack_contract_plan_artifact_row(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for plan artifact rows."""
    row = _sample_plan_artifact_row()
    payload = encode_msgpack(row)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "plan_artifact_row.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=PlanArtifactRow)
    assert decoded == row


def _sample_plan_schedule_artifact() -> PlanScheduleArtifact:
    return PlanScheduleArtifact(
        run_id="run-1",
        plan_signature="sig-1",
        reduced_plan_signature="sig-1:reduced",
        task_count=3,
        ordered_tasks=("task_a", "task_b", "task_c"),
        generations=(("task_a",), ("task_b", "task_c")),
        critical_path_tasks=("task_a", "task_c"),
        critical_path_length_weighted=3.5,
        task_costs={"task_a": 1.0, "task_b": 1.5, "task_c": 2.0},
        bottom_level_costs={"task_a": 3.5, "task_b": 1.5, "task_c": 2.0},
        slack_by_task={"task_a": 0.0, "task_b": 1.0, "task_c": 0.0},
    )


def test_msgpack_contract_plan_schedule_artifact(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for plan schedule artifacts."""
    artifact = _sample_plan_schedule_artifact()
    payload = encode_msgpack(artifact)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "plan_schedule_artifact.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=PlanScheduleArtifact)
    assert decoded == artifact


def test_msgpack_contract_plan_schedule_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for plan schedule envelopes."""
    envelope = PlanScheduleEnvelope(payload=_sample_plan_schedule_artifact())
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "plan_schedule_artifact.envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=PlanScheduleEnvelope)
    assert decoded == envelope


def _sample_plan_validation_artifact() -> PlanValidationArtifact:
    return PlanValidationArtifact(
        run_id="run-1",
        plan_signature="sig-1",
        reduced_plan_signature="sig-1:reduced",
        total_tasks=2,
        valid_tasks=1,
        invalid_tasks=1,
        total_edges=2,
        valid_edges=1,
        invalid_edges=1,
        task_results=(
            {
                "task_name": "task_a",
                "is_valid": True,
                "unsatisfied_edges": [],
                "contract_violations": [],
                "edge_results": [],
            },
            {
                "task_name": "task_b",
                "is_valid": False,
                "unsatisfied_edges": ["input_x"],
                "contract_violations": ["violation"],
                "edge_results": [
                    {
                        "source_name": "input_x",
                        "target_task": "task_b",
                        "is_valid": False,
                        "missing_columns": ["col_x"],
                        "missing_types": [{"column": "col_x", "dtype": "Utf8"}],
                        "missing_metadata": [],
                        "contract_violations": ["violation"],
                        "available_columns": [],
                        "available_types": [],
                        "available_metadata": [],
                    }
                ],
            },
        ),
    )


def test_msgpack_contract_plan_validation_artifact(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for plan validation artifacts."""
    artifact = _sample_plan_validation_artifact()
    payload = encode_msgpack(artifact)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "plan_validation_artifact.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=PlanValidationArtifact)
    assert decoded == artifact


def test_msgpack_contract_plan_validation_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for plan validation envelopes."""
    envelope = PlanValidationEnvelope(payload=_sample_plan_validation_artifact())
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "plan_validation_artifact.envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=PlanValidationEnvelope)
    assert decoded == envelope


def _sample_delta_write_policy() -> DeltaWritePolicy:
    return DeltaWritePolicy(
        target_file_size=256,
        partition_by=("repo",),
        zorder_by=("node_id",),
        stats_policy="explicit",
        stats_columns=("node_id",),
        stats_max_columns=32,
        parquet_writer_policy=ParquetWriterPolicy(
            statistics_enabled=("node_id",),
            statistics_level="page",
            bloom_filter_enabled=("node_id",),
            bloom_filter_fpp=0.02,
            bloom_filter_ndv=500,
            dictionary_enabled=("node_kind",),
        ),
        enable_features=("change_data_feed",),
    )


def test_msgpack_contract_delta_write_policy(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for Delta write policy."""
    policy = _sample_delta_write_policy()
    payload = encode_msgpack(policy)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "delta_write_policy.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=DeltaWritePolicy)
    assert decoded == policy


def _sample_parquet_writer_policy() -> ParquetWriterPolicy:
    return ParquetWriterPolicy(
        statistics_enabled=("node_id", "path"),
        statistics_level="page",
        bloom_filter_enabled=("node_id",),
        bloom_filter_fpp=0.01,
        bloom_filter_ndv=1000,
        dictionary_enabled=("node_kind",),
    )


def test_msgpack_contract_parquet_writer_policy(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for Parquet writer policy."""
    policy = _sample_parquet_writer_policy()
    payload = encode_msgpack(policy)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "parquet_writer_policy.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=ParquetWriterPolicy)
    assert decoded == policy


def _sample_delta_schema_policy() -> DeltaSchemaPolicy:
    return DeltaSchemaPolicy(
        schema_mode="merge",
        column_mapping_mode="name",
    )


def test_msgpack_contract_delta_schema_policy(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for Delta schema policy."""
    policy = _sample_delta_schema_policy()
    payload = encode_msgpack(policy)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "delta_schema_policy.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=DeltaSchemaPolicy)
    assert decoded == policy


def _sample_delta_stats_decision() -> DeltaStatsDecision:
    return DeltaStatsDecision(
        dataset_name="cpg_nodes",
        stats_policy="auto",
        stats_columns=("file_id", "node_id"),
        lineage_columns=("file_id", "node_kind"),
        partition_by=("node_kind",),
        zorder_by=("file_id", "node_id"),
        stats_max_columns=32,
    )


def test_msgpack_contract_delta_stats_decision(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for Delta stats decisions."""
    decision = _sample_delta_stats_decision()
    payload = encode_msgpack(decision)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "delta_stats_decision.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=DeltaStatsDecision)
    assert decoded == decision


def test_msgpack_contract_delta_stats_decision_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for Delta stats decision envelopes."""
    envelope = DeltaStatsDecisionEnvelope(payload=_sample_delta_stats_decision())
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "delta_stats_decision.envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=DeltaStatsDecisionEnvelope)
    assert decoded == envelope


def _sample_view_cache_artifact() -> ViewCacheArtifact:
    return ViewCacheArtifact(
        view_name="cpg_edges_v1",
        cache_policy="delta_staging",
        cache_path="/tmp/datafusion_view_cache/cpg_edges_v1__fp-1",
        plan_fingerprint="fp-1",
        status="cached",
        hit=None,
    )


def test_msgpack_contract_view_cache_artifact(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for view cache artifacts."""
    artifact = _sample_view_cache_artifact()
    payload = encode_msgpack(artifact)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "view_cache_artifact.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=ViewCacheArtifact)
    assert decoded == artifact


def test_msgpack_contract_view_cache_artifact_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for view cache artifact envelopes."""
    envelope = ViewCacheArtifactEnvelope(payload=_sample_view_cache_artifact())
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "view_cache_artifact.envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=ViewCacheArtifactEnvelope)
    assert decoded == envelope


def _sample_semantic_validation_artifact() -> SemanticValidationArtifact:
    return SemanticValidationArtifact(
        view_name="cpg_nodes_v1",
        status="ok",
        entries=(
            SemanticValidationEntry(
                column_name="node_id",
                expected="NodeId",
                actual="NodeId",
                status="ok",
            ),
        ),
        errors=(),
    )


def test_msgpack_contract_semantic_validation_artifact(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for semantic validation artifacts."""
    artifact = _sample_semantic_validation_artifact()
    payload = encode_msgpack(artifact)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "semantic_validation_artifact.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=SemanticValidationArtifact)
    assert decoded == artifact


def test_msgpack_contract_semantic_validation_artifact_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for semantic validation envelopes."""
    envelope = SemanticValidationArtifactEnvelope(payload=_sample_semantic_validation_artifact())
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "semantic_validation_artifact.envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=SemanticValidationArtifactEnvelope)
    assert decoded == envelope


def _sample_run_manifest() -> RunManifest:
    return RunManifest(
        run_id="run-1",
        status="completed",
        event_time_unix_ms=1735689600000,
        plan_signature="sig-1",
        plan_fingerprints={"cpg_nodes_v1": "fp-1"},
        delta_inputs=(
            {
                "dataset_name": "cpg_nodes",
                "version": 1,
                "timestamp": None,
            },
        ),
        outputs=(
            {
                "name": "cpg_nodes",
                "path": "/tmp/out/cpg_nodes",
                "delta_version": 1,
                "rows": 10,
            },
        ),
        runtime_profile_name="default",
        runtime_profile_hash="profile-hash",
        determinism_tier="strict",
        output_dir="/tmp/out",
        artifact_ids={
            "plan_schedule": "schedule-1",
            "plan_validation": "validation-1",
            "cpg_nodes": "pid-1",
        },
    )


def test_msgpack_contract_run_manifest(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for run manifest artifacts."""
    manifest = _sample_run_manifest()
    payload = encode_msgpack(manifest)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "run_manifest.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=RunManifest)
    assert decoded == manifest


def test_msgpack_contract_run_manifest_envelope(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for run manifest envelopes."""
    envelope = RunManifestEnvelope(payload=_sample_run_manifest())
    payload = encode_msgpack(envelope)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "run_manifest.envelope.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=RunManifestEnvelope)
    assert decoded == envelope


def _sample_plan_proto_status() -> PlanProtoStatus:
    return PlanProtoStatus(
        enabled=False,
        installed=False,
        reason="delta_plan_codecs_unavailable",
    )


def test_msgpack_contract_plan_proto_status(*, update_goldens: bool) -> None:
    """Snapshot MessagePack payloads for plan proto status."""
    status = _sample_plan_proto_status()
    payload = encode_msgpack(status)
    assert_bytes_snapshot_b64(
        path=GOLDENS_DIR / "plan_proto_status.msgpack.b64",
        data=payload,
        update=update_goldens,
    )

    decoded = decode_msgpack(payload, target_type=PlanProtoStatus)
    assert decoded == status
