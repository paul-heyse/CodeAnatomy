"""Error contract tests for msgspec payloads."""

from __future__ import annotations

import datetime as dt

import msgspec
import pytest

from serde_artifacts import (
    DeltaStatsDecision,
    PlanScheduleArtifact,
    PlanValidationArtifact,
    RunManifest,
    SemanticValidationArtifact,
    ViewCacheArtifact,
)
from storage.deltalake.config import DeltaWritePolicy, ParquetWriterPolicy
from tests.msgspec_contract._support.codecs import decode_json
from tests.msgspec_contract._support.goldens import GOLDENS_DIR, assert_text_snapshot
from tests.msgspec_contract._support.models import StrictUser
from tests.msgspec_contract._support.normalize import normalize_exception


def test_error_contract_missing_field(*, update_goldens: bool) -> None:
    """Snapshot missing-field validation errors."""
    payload = {
        "id": "user-3",
        "created_at": dt.datetime(2025, 3, 3, 0, 0, 0, tzinfo=dt.UTC).isoformat(),
    }
    try:
        decode_json(msgspec.json.encode(payload), target_type=StrictUser)
    except msgspec.ValidationError as exc:
        normalized = normalize_exception(exc)
        assert_text_snapshot(
            path=GOLDENS_DIR / "strict_user_missing_field.error.json",
            text=msgspec.json.format(msgspec.json.encode(normalized), indent=2).decode("utf-8"),
            update=update_goldens,
        )
        return
    msg = "Expected msgspec.ValidationError"
    pytest.fail(msg)


def test_error_contract_unknown_field(*, update_goldens: bool) -> None:
    """Snapshot unknown-field validation errors."""
    payload = {
        "id": "user-4",
        "name": "Linus",
        "created_at": dt.datetime(2025, 3, 3, 0, 0, 0, tzinfo=dt.UTC).isoformat(),
        "extra": "boom",
    }
    try:
        decode_json(msgspec.json.encode(payload), target_type=StrictUser)
    except msgspec.ValidationError as exc:
        normalized = normalize_exception(exc)
        assert_text_snapshot(
            path=GOLDENS_DIR / "strict_user_unknown_field.error.json",
            text=msgspec.json.format(msgspec.json.encode(normalized), indent=2).decode("utf-8"),
            update=update_goldens,
        )
        return
    msg = "Expected msgspec.ValidationError"
    pytest.fail(msg)


def _assert_error_snapshot(
    *,
    payload: dict[str, object],
    target_type: type[object],
    snapshot_name: str,
    update_goldens: bool,
) -> None:
    try:
        decode_json(msgspec.json.encode(payload), target_type=target_type)
    except msgspec.ValidationError as exc:
        normalized = normalize_exception(exc)
        assert_text_snapshot(
            path=GOLDENS_DIR / snapshot_name,
            text=msgspec.json.format(msgspec.json.encode(normalized), indent=2).decode("utf-8"),
            update=update_goldens,
        )
        return
    msg = "Expected msgspec.ValidationError"
    pytest.fail(msg)


def test_error_contract_delta_stats_decision(*, update_goldens: bool) -> None:
    """Snapshot validation errors for delta stats decisions."""
    _assert_error_snapshot(
        payload={
            "dataset_name": "",
            "stats_policy": "auto",
            "stats_columns": [],
            "lineage_columns": [],
        },
        target_type=DeltaStatsDecision,
        snapshot_name="delta_stats_decision.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_view_cache_artifact(*, update_goldens: bool) -> None:
    """Snapshot validation errors for view cache artifacts."""
    _assert_error_snapshot(
        payload={
            "view_name": "",
            "cache_policy": "",
            "cache_path": None,
            "plan_fingerprint": None,
            "status": "",
        },
        target_type=ViewCacheArtifact,
        snapshot_name="view_cache_artifact.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_semantic_validation_artifact(*, update_goldens: bool) -> None:
    """Snapshot validation errors for semantic validation artifacts."""
    _assert_error_snapshot(
        payload={
            "view_name": "",
            "status": "ok",
            "entries": [
                {
                    "column_name": "",
                    "expected": "",
                    "actual": None,
                    "status": "missing",
                }
            ],
            "errors": [],
        },
        target_type=SemanticValidationArtifact,
        snapshot_name="semantic_validation_artifact.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_run_manifest(*, update_goldens: bool) -> None:
    """Snapshot validation errors for run manifest artifacts."""
    _assert_error_snapshot(
        payload={
            "run_id": "",
            "status": "",
            "event_time_unix_ms": -1,
            "plan_signature": None,
            "plan_fingerprints": {},
            "delta_inputs": [],
            "outputs": [],
            "runtime_profile_name": None,
            "runtime_profile_hash": None,
            "determinism_tier": None,
            "output_dir": None,
        },
        target_type=RunManifest,
        snapshot_name="run_manifest.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_run_manifest_fingerprint(*, update_goldens: bool) -> None:
    """Snapshot validation errors for invalid plan fingerprint values."""
    _assert_error_snapshot(
        payload={
            "run_id": "run_01HZX4J3C8F8M2KQ",
            "status": "ok",
            "event_time_unix_ms": 1735689600000,
            "plan_signature": "sig-1",
            "plan_fingerprints": {"cpg_nodes_v1": "not-a-hex"},
            "delta_inputs": [],
            "outputs": [],
            "runtime_profile_name": None,
            "runtime_profile_hash": None,
            "determinism_tier": None,
            "output_dir": None,
        },
        target_type=RunManifest,
        snapshot_name="run_manifest.plan_fingerprint.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_plan_schedule_artifact(*, update_goldens: bool) -> None:
    """Snapshot validation errors for plan schedule artifacts."""
    _assert_error_snapshot(
        payload={
            "run_id": "",
            "plan_signature": "",
            "reduced_plan_signature": "",
            "task_count": -1,
            "ordered_tasks": [],
            "generations": [],
            "critical_path_tasks": [],
            "critical_path_length_weighted": None,
            "task_costs": {},
            "bottom_level_costs": {},
        },
        target_type=PlanScheduleArtifact,
        snapshot_name="plan_schedule_artifact.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_plan_validation_artifact(*, update_goldens: bool) -> None:
    """Snapshot validation errors for plan validation artifacts."""
    _assert_error_snapshot(
        payload={
            "run_id": "",
            "plan_signature": "",
            "reduced_plan_signature": "",
            "total_tasks": -1,
            "valid_tasks": 0,
            "invalid_tasks": 0,
            "total_edges": 0,
            "valid_edges": 0,
            "invalid_edges": 0,
            "task_results": [],
        },
        target_type=PlanValidationArtifact,
        snapshot_name="plan_validation_artifact.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_delta_write_policy(*, update_goldens: bool) -> None:
    """Snapshot validation errors for Delta write policies."""
    _assert_error_snapshot(
        payload={
            "target_file_size": 0,
            "partition_by": [""],
            "zorder_by": [],
            "stats_policy": "auto",
            "stats_columns": [],
            "stats_max_columns": -1,
            "parquet_writer_policy": None,
            "enable_features": [],
        },
        target_type=DeltaWritePolicy,
        snapshot_name="delta_write_policy.error.json",
        update_goldens=update_goldens,
    )


def test_error_contract_delta_write_policy_invalid(*, update_goldens: bool) -> None:
    """Snapshot validation errors for invalid Delta write policies."""
    payload = {"target_file_size": -5}
    try:
        decode_json(msgspec.json.encode(payload), target_type=DeltaWritePolicy)
    except msgspec.ValidationError as exc:
        normalized = normalize_exception(exc)
        assert_text_snapshot(
            path=GOLDENS_DIR / "delta_write_policy_invalid.error.json",
            text=msgspec.json.format(msgspec.json.encode(normalized), indent=2).decode("utf-8"),
            update=update_goldens,
        )
        return
    msg = "Expected msgspec.ValidationError"
    pytest.fail(msg)


def test_error_contract_parquet_writer_policy_invalid(*, update_goldens: bool) -> None:
    """Snapshot validation errors for invalid Parquet writer policies."""
    payload = {"bloom_filter_fpp": -0.1}
    try:
        decode_json(msgspec.json.encode(payload), target_type=ParquetWriterPolicy)
    except msgspec.ValidationError as exc:
        normalized = normalize_exception(exc)
        assert_text_snapshot(
            path=GOLDENS_DIR / "parquet_writer_policy_invalid.error.json",
            text=msgspec.json.format(msgspec.json.encode(normalized), indent=2).decode("utf-8"),
            update=update_goldens,
        )
        return
    msg = "Expected msgspec.ValidationError"
    pytest.fail(msg)
