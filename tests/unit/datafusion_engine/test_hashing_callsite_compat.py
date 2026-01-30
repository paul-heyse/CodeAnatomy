"""Compatibility tests for hashing call sites."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import cast

from datafusion_engine.delta_scan_config import delta_scan_identity_hash
from datafusion_engine.plan_artifact_store import _plan_identity_payload
from datafusion_engine.plan_bundle import (
    DataFusionPlanBundle,
    PlanFingerprintInputs,
    _hash_plan,
    _information_schema_hash,
    _planning_env_hash,
    _rulepack_hash,
)
from datafusion_engine.plan_bundle import (
    _delta_protocol_payload as plan_delta_protocol_payload,
)
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.scan_planner import ScanUnit
from datafusion_engine.view_artifacts import _delta_inputs_payload, _plan_task_signature
from extract.cache_utils import CACHE_VERSION, stable_cache_key, stable_cache_label
from extract.repo_scan import _sha256_path
from incremental.scip_fingerprint import scip_index_fingerprint
from relspec.execution_plan import _protocol_payload as exec_protocol_payload
from relspec.execution_plan import _scan_unit_signature
from serde_artifacts import DeltaInputPin, DeltaScanConfigSnapshot, PlanArtifacts
from serde_msgspec import JSON_ENCODER, MSGPACK_ENCODER, to_builtins
from utils.hashing import hash_json_default


def test_planning_env_hash_matches_msgpack_encoder() -> None:
    """Hash planning env snapshots with canonical msgpack encoding."""
    snapshot = {"b": 2, "a": 1}
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(snapshot)).hexdigest()
    assert _planning_env_hash(snapshot) == expected


def test_rulepack_hash_matches_msgpack_encoder() -> None:
    """Hash rulepack snapshots with canonical msgpack encoding."""
    assert _rulepack_hash(None) is None
    snapshot = {"status": "ok", "rules": ["a", "b"]}
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(snapshot)).hexdigest()
    assert _rulepack_hash(snapshot) == expected


def test_information_schema_hash_matches_msgpack_encoder() -> None:
    """Hash information schema snapshots with canonical msgpack encoding."""
    columns: list[dict[str, object]] = []
    snapshot: dict[str, object] = {"tables": [{"name": "t1"}], "columns": columns}
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(snapshot)).hexdigest()
    assert _information_schema_hash(snapshot) == expected


def test_hash_plan_matches_legacy_payload() -> None:
    """Hash plan fingerprints with the legacy payload encoding."""
    substrait = b"substrait"
    df_settings = {"b": "2", "a": "1"}
    pin = DeltaInputPin(
        dataset_name="demo",
        version=7,
        timestamp="123",
        protocol=None,
        delta_scan_config=None,
        delta_scan_config_hash="scan_hash",
        datafusion_provider="provider",
        protocol_compatible=True,
        protocol_compatibility=None,
    )
    inputs = PlanFingerprintInputs(
        substrait_bytes=substrait,
        df_settings=df_settings,
        planning_env_hash="env",
        rulepack_hash=None,
        information_schema_hash="info",
        udf_snapshot_hash="udf",
        required_udfs=("udf_b", "udf_a"),
        required_rewrite_tags=("tag_a",),
        delta_inputs=(pin,),
        delta_store_policy_hash="policy",
    )
    settings_items = tuple(sorted(df_settings.items()))
    settings_hash = hashlib.sha256(MSGPACK_ENCODER.encode(settings_items)).hexdigest()
    substrait_hash = hashlib.sha256(substrait).hexdigest()
    delta_payload = tuple(
        sorted(
            (
                (
                    pin.dataset_name,
                    pin.version,
                    pin.timestamp,
                    plan_delta_protocol_payload(pin.protocol),
                    pin.delta_scan_config_hash,
                    pin.datafusion_provider,
                    pin.protocol_compatible,
                ),
            ),
            key=lambda item: item[0],
        )
    )
    payload = (
        ("substrait_hash", substrait_hash),
        ("settings_hash", settings_hash),
        ("planning_env_hash", inputs.planning_env_hash or ""),
        ("rulepack_hash", inputs.rulepack_hash or ""),
        ("information_schema_hash", inputs.information_schema_hash or ""),
        ("udf_snapshot_hash", inputs.udf_snapshot_hash),
        ("required_udfs", tuple(sorted(inputs.required_udfs))),
        ("required_rewrite_tags", tuple(sorted(inputs.required_rewrite_tags))),
        ("delta_inputs", delta_payload),
        ("delta_store_policy_hash", inputs.delta_store_policy_hash),
    )
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(payload)).hexdigest()
    assert _hash_plan(inputs) == expected


def test_delta_scan_config_hash_matches_msgpack_encoder() -> None:
    """Hash delta scan config snapshots with canonical msgpack encoding."""
    snapshot = DeltaScanConfigSnapshot(
        file_column_name="path",
        enable_parquet_pushdown=False,
        schema_force_view_types=True,
        wrap_partition_values=True,
        schema={"fields": [{"name": "id"}]},
    )
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(snapshot)).hexdigest()
    assert delta_scan_identity_hash(snapshot) == expected


def test_extract_cache_key_matches_msgpack_encoder() -> None:
    """Build extract cache keys using the canonical msgpack digest."""
    payload = {"name": "cache", "count": 3}
    enriched = {"version": CACHE_VERSION, **payload}
    digest = hashlib.sha256(MSGPACK_ENCODER.encode(to_builtins(enriched))).hexdigest()
    assert stable_cache_key("prefix", payload) == f"prefix:{digest}"
    assert stable_cache_label("prefix", payload) == f"prefix_{digest}"


def test_extract_repo_scan_sha256_path(tmp_path: Path) -> None:
    """Hash repo scan inputs with the canonical SHA-256 file digest."""
    path = tmp_path / "payload.txt"
    path.write_text("hash-me", encoding="utf-8")
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    assert _sha256_path(path) == expected


def test_scip_index_fingerprint_matches_chunked_sha256(tmp_path: Path) -> None:
    """Hash SCIP indexes with chunked SHA-256 semantics."""
    path = tmp_path / "index.scip"
    path.write_bytes(b"x" * 4096)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    assert scip_index_fingerprint(path) == expected


def test_scan_unit_signature_matches_msgpack_encoder() -> None:
    """Hash scan unit signatures with canonical msgpack encoding."""
    scan_unit = ScanUnit(
        key="scan_key",
        dataset_name="dataset",
        delta_version=3,
        delta_timestamp="456",
        snapshot_timestamp=789,
        delta_protocol=None,
        delta_scan_config=None,
        delta_scan_config_hash="scan_hash",
        datafusion_provider=None,
        protocol_compatible=None,
        protocol_compatibility=None,
        total_files=10,
        candidate_file_count=3,
        pruned_file_count=7,
        candidate_files=(Path("a.parquet"), Path("b.parquet")),
        pushed_filters=("col = 1",),
        projected_columns=("col",),
    )
    candidate_files = tuple(sorted(str(path) for path in scan_unit.candidate_files))
    payload = (
        ("version", 1),
        ("runtime_hash", "runtime"),
        ("scan_key", scan_unit.key),
        ("dataset_name", scan_unit.dataset_name),
        ("delta_version", scan_unit.delta_version),
        ("delta_timestamp", scan_unit.delta_timestamp),
        ("snapshot_timestamp", scan_unit.snapshot_timestamp),
        ("delta_protocol", exec_protocol_payload(scan_unit.delta_protocol)),
        ("total_files", scan_unit.total_files),
        ("candidate_file_count", scan_unit.candidate_file_count),
        ("pruned_file_count", scan_unit.pruned_file_count),
        ("candidate_files", candidate_files),
        ("pushed_filters", tuple(sorted(scan_unit.pushed_filters))),
        ("projected_columns", tuple(sorted(scan_unit.projected_columns))),
    )
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(payload)).hexdigest()
    assert _scan_unit_signature(scan_unit, runtime_hash="runtime") == expected


def test_plan_task_signature_matches_msgpack_encoder() -> None:
    """Hash plan task signatures with canonical msgpack encoding."""
    artifacts = PlanArtifacts(
        explain_tree_rows=None,
        explain_verbose_rows=None,
        explain_analyze_duration_ms=None,
        explain_analyze_output_rows=None,
        df_settings={"a": "1"},
        planning_env_snapshot={},
        planning_env_hash="planning_hash",
        rulepack_snapshot=None,
        rulepack_hash=None,
        information_schema_snapshot={},
        information_schema_hash="info_hash",
        substrait_validation=None,
        logical_plan_proto=None,
        optimized_plan_proto=None,
        execution_plan_proto=None,
        udf_snapshot_hash="udf_hash",
        function_registry_hash="registry_hash",
        rewrite_tags=("tag",),
        domain_planner_names=("domain",),
        udf_snapshot={},
        udf_planner_snapshot=None,
    )
    delta_inputs = (
        DeltaInputPin(
            dataset_name="dataset",
            version=1,
            timestamp="123",
            protocol=None,
            delta_scan_config=None,
            delta_scan_config_hash=None,
            datafusion_provider=None,
            protocol_compatible=None,
            protocol_compatibility=None,
        ),
    )
    bundle_stub = SimpleNamespace(
        artifacts=artifacts,
        plan_fingerprint="plan_fp",
        required_udfs=("udf",),
        required_rewrite_tags=("rewrite",),
        delta_inputs=delta_inputs,
    )
    bundle = cast("DataFusionPlanBundle", bundle_stub)
    settings_items = tuple(sorted(artifacts.df_settings.items()))
    df_settings_hash = hashlib.sha256(MSGPACK_ENCODER.encode(settings_items)).hexdigest()
    payload = (
        ("version", 2),
        ("runtime_hash", "runtime"),
        ("plan_fingerprint", "plan_fp"),
        ("function_registry_hash", artifacts.function_registry_hash),
        ("udf_snapshot_hash", artifacts.udf_snapshot_hash),
        ("planning_env_hash", artifacts.planning_env_hash),
        ("rulepack_hash", artifacts.rulepack_hash or ""),
        ("information_schema_hash", artifacts.information_schema_hash),
        ("rewrite_tags", tuple(sorted(artifacts.rewrite_tags))),
        ("domain_planner_names", tuple(sorted(artifacts.domain_planner_names))),
        ("df_settings_hash", df_settings_hash),
        ("required_udfs", ("udf",)),
        ("required_rewrite_tags", ("rewrite",)),
        ("delta_inputs", _delta_inputs_payload(cast("DataFusionPlanBundle", bundle_stub))),
    )
    expected = hashlib.sha256(MSGPACK_ENCODER.encode(payload)).hexdigest()
    assert _plan_task_signature(bundle, runtime_hash="runtime") == expected


def test_plan_identity_hash_matches_json_encoder() -> None:
    """Hash plan identity payloads with canonical JSON encoding."""
    artifacts = PlanArtifacts(
        explain_tree_rows=None,
        explain_verbose_rows=None,
        explain_analyze_duration_ms=None,
        explain_analyze_output_rows=None,
        df_settings={"a": "1", "b": "2"},
        planning_env_snapshot={},
        planning_env_hash="planning_hash",
        rulepack_snapshot=None,
        rulepack_hash=None,
        information_schema_snapshot={},
        information_schema_hash="info_hash",
        substrait_validation=None,
        logical_plan_proto=None,
        optimized_plan_proto=None,
        execution_plan_proto=None,
        udf_snapshot_hash="udf_hash",
        function_registry_hash="registry_hash",
        rewrite_tags=(),
        domain_planner_names=("domain",),
        udf_snapshot={},
        udf_planner_snapshot=None,
    )
    bundle_stub = SimpleNamespace(
        artifacts=artifacts,
        plan_fingerprint="plan_fp",
        required_udfs=("udf",),
        required_rewrite_tags=(),
        delta_inputs=(),
    )
    delta_inputs_payload: tuple[dict[str, object], ...] = (
        {
            "dataset_name": "dataset",
            "version": 1,
            "timestamp": "123",
            "protocol": None,
            "delta_scan_config_hash": None,
            "datafusion_provider": None,
            "protocol_compatible": None,
            "protocol_compatibility": None,
        },
    )
    scan_payload = ({"dataset_name": "dataset", "key": "scan_key"},)
    payload = _plan_identity_payload(
        bundle=cast("DataFusionPlanBundle", bundle_stub),
        profile=DataFusionRuntimeProfile(),
        delta_inputs_payload=delta_inputs_payload,
        scan_payload=scan_payload,
        scan_keys_payload=["scan_key"],
    )
    buffer = bytearray()
    JSON_ENCODER.encode_into(to_builtins(payload), buffer)
    expected = hashlib.sha256(buffer).hexdigest()
    assert hash_json_default(payload) == expected
