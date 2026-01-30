"""JSON contract tests for msgspec payloads."""

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
from serde_msgspec import decode_json_lines, dumps_msgpack, encode_json_lines
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy, ParquetWriterPolicy
from tests.msgspec_contract._support.codecs import decode_json, encode_json_pretty
from tests.msgspec_contract._support.goldens import GOLDENS_DIR, assert_text_snapshot
from tests.msgspec_contract._support.models import Envelope, Purchase, User


def _sample_envelope() -> Envelope:
    return Envelope(
        user=User(
            id="user-1",
            name="Ada",
            created_at=dt.datetime(2025, 1, 1, 12, 0, 0, tzinfo=dt.UTC),
        ),
        event=Purchase(
            ts=dt.datetime(2025, 1, 1, 12, 5, 0, tzinfo=dt.UTC),
            amount_cents=2500,
            currency="USD",
        ),
        metadata={"source": "tests"},
    )


def test_json_contract_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for the envelope contract."""
    envelope = _sample_envelope()
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=Envelope)
    assert decoded == envelope


def test_json_contract_envelope_lines() -> None:
    """Validate JSON Lines encode/decode for typed payloads."""
    envelopes = (_sample_envelope(), _sample_envelope())
    payload = encode_json_lines(list(envelopes))
    decoded = decode_json_lines(payload, target_type=Envelope)
    assert decoded == list(envelopes)


def _sample_plan_artifact_row() -> PlanArtifactRow:
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
        plan_fingerprint="4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c",
        plan_identity_hash=("9b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c"),
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
        substrait_msgpack=dumps_msgpack(b"substrait"),
        logical_plan_proto_msgpack=None,
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
        udf_snapshot_msgpack=dumps_msgpack({"hash": "udf-hash"}),
        udf_planner_snapshot_msgpack=None,
        udf_compatibility_ok=True,
        udf_compatibility_detail_msgpack=dumps_msgpack({"status": "ok"}),
        execution_duration_ms=None,
        execution_status=None,
        execution_error=None,
    )


def test_json_contract_plan_artifact_row(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for plan artifact rows."""
    row = _sample_plan_artifact_row()
    text = encode_json_pretty(row, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "plan_artifact_row.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=PlanArtifactRow)
    assert decoded == row


def _sample_delta_write_policy() -> DeltaWritePolicy:
    return DeltaWritePolicy(
        target_file_size=128,
        partition_by=("repo",),
        zorder_by=("node_id",),
        stats_policy="explicit",
        stats_columns=("node_id", "path"),
        stats_max_columns=16,
        parquet_writer_policy=ParquetWriterPolicy(
            statistics_enabled=("node_id", "path"),
            statistics_level="page",
            bloom_filter_enabled=("node_id",),
            bloom_filter_fpp=0.01,
            bloom_filter_ndv=1000,
            dictionary_enabled=("node_kind",),
        ),
        enable_features=("change_data_feed",),
    )


def test_json_contract_delta_write_policy(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for Delta write policy."""
    policy = _sample_delta_write_policy()
    text = encode_json_pretty(policy, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "delta_write_policy.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=DeltaWritePolicy)
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


def test_json_contract_parquet_writer_policy(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for Parquet writer policy."""
    policy = _sample_parquet_writer_policy()
    text = encode_json_pretty(policy, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "parquet_writer_policy.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=ParquetWriterPolicy)
    assert decoded == policy


def _sample_delta_schema_policy() -> DeltaSchemaPolicy:
    return DeltaSchemaPolicy(
        schema_mode="merge",
        column_mapping_mode="name",
    )


def test_json_contract_delta_schema_policy(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for Delta schema policy."""
    policy = _sample_delta_schema_policy()
    text = encode_json_pretty(policy, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "delta_schema_policy.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=DeltaSchemaPolicy)
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


def test_json_contract_delta_stats_decision(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for Delta stats decisions."""
    decision = _sample_delta_stats_decision()
    text = encode_json_pretty(decision, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "delta_stats_decision.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=DeltaStatsDecision)
    assert decoded == decision


def test_json_contract_delta_stats_decision_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for Delta stats decision envelopes."""
    envelope = DeltaStatsDecisionEnvelope(payload=_sample_delta_stats_decision())
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "delta_stats_decision.envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=DeltaStatsDecisionEnvelope)
    assert decoded == envelope


def _sample_view_cache_artifact() -> ViewCacheArtifact:
    return ViewCacheArtifact(
        view_name="cpg_edges_v1",
        cache_policy="delta_staging",
        cache_path=("/tmp/datafusion_view_cache/cpg_edges_v1__4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c"),
        plan_fingerprint="4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c",
        status="cached",
        hit=None,
    )


def test_json_contract_view_cache_artifact(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for view cache artifacts."""
    artifact = _sample_view_cache_artifact()
    text = encode_json_pretty(artifact, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "view_cache_artifact.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=ViewCacheArtifact)
    assert decoded == artifact


def test_json_contract_view_cache_artifact_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for view cache artifact envelopes."""
    envelope = ViewCacheArtifactEnvelope(payload=_sample_view_cache_artifact())
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "view_cache_artifact.envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=ViewCacheArtifactEnvelope)
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


def test_json_contract_semantic_validation_artifact(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for semantic validation artifacts."""
    artifact = _sample_semantic_validation_artifact()
    text = encode_json_pretty(artifact, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "semantic_validation_artifact.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=SemanticValidationArtifact)
    assert decoded == artifact


def test_json_contract_semantic_validation_artifact_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for semantic validation envelopes."""
    envelope = SemanticValidationArtifactEnvelope(payload=_sample_semantic_validation_artifact())
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "semantic_validation_artifact.envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=SemanticValidationArtifactEnvelope)
    assert decoded == envelope


def _sample_plan_schedule_artifact() -> PlanScheduleArtifact:
    return PlanScheduleArtifact(
        run_id="run_01HZX4J3C8F8M2KQ",
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


def test_json_contract_plan_schedule_artifact(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for plan schedule artifacts."""
    artifact = _sample_plan_schedule_artifact()
    text = encode_json_pretty(artifact, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "plan_schedule_artifact.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=PlanScheduleArtifact)
    assert decoded == artifact


def test_json_contract_plan_schedule_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for plan schedule envelopes."""
    envelope = PlanScheduleEnvelope(payload=_sample_plan_schedule_artifact())
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "plan_schedule_artifact.envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=PlanScheduleEnvelope)
    assert decoded == envelope


def _sample_plan_validation_artifact() -> PlanValidationArtifact:
    return PlanValidationArtifact(
        run_id="run_01HZX4J3C8F8M2KQ",
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


def test_json_contract_plan_validation_artifact(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for plan validation artifacts."""
    artifact = _sample_plan_validation_artifact()
    text = encode_json_pretty(artifact, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "plan_validation_artifact.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=PlanValidationArtifact)
    assert decoded == artifact


def test_json_contract_plan_validation_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for plan validation envelopes."""
    envelope = PlanValidationEnvelope(payload=_sample_plan_validation_artifact())
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "plan_validation_artifact.envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=PlanValidationEnvelope)
    assert decoded == envelope


def _sample_run_manifest() -> RunManifest:
    return RunManifest(
        run_id="run_01HZX4J3C8F8M2KQ",
        status="completed",
        event_time_unix_ms=1735689600000,
        plan_signature="sig-1",
        plan_fingerprints={"cpg_nodes_v1": "4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c"},
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
            "cpg_nodes": "9b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e",
        },
    )


def test_json_contract_run_manifest(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for run manifest artifacts."""
    manifest = _sample_run_manifest()
    text = encode_json_pretty(manifest, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "run_manifest.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=RunManifest)
    assert decoded == manifest


def test_json_contract_run_manifest_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for run manifest envelopes."""
    envelope = RunManifestEnvelope(payload=_sample_run_manifest())
    text = encode_json_pretty(envelope, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "run_manifest.envelope.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=RunManifestEnvelope)
    assert decoded == envelope


def _sample_plan_proto_status() -> PlanProtoStatus:
    return PlanProtoStatus(
        enabled=False,
        installed=False,
        reason="delta_plan_codecs_unavailable",
    )


def test_json_contract_plan_proto_status(*, update_goldens: bool) -> None:
    """Snapshot JSON payloads for plan proto status."""
    status = _sample_plan_proto_status()
    text = encode_json_pretty(status, indent=2)
    assert_text_snapshot(
        path=GOLDENS_DIR / "plan_proto_status.json",
        text=text,
        update=update_goldens,
    )

    decoded = decode_json(text, target_type=PlanProtoStatus)
    assert decoded == status
