"""Serialization helpers for plan artifacts."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import msgspec

from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from serde_msgspec import dumps_msgpack, ensure_raw, to_builtins
from serde_msgspec_ext import SubstraitBytes
from utils.hashing import hash_json_default

if TYPE_CHECKING:
    from typing import Protocol

    from datafusion import SessionContext

    from datafusion_engine.lineage.reporting import LineageReport
    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

    class PipelineEventRowLike(Protocol):
        event_name: str
        run_id: str


def bundle_payload(bundle: DataFusionPlanArtifact) -> Mapping[str, object]:
    """Return canonical payload representation for a plan bundle."""
    return {
        "plan_identity_hash": bundle.plan_identity_hash,
        "plan_fingerprint": bundle.plan_fingerprint,
        "substrait_bytes": bundle.substrait_bytes,
        "has_execution_plan": bundle.execution_plan is not None,
    }


def commit_metadata_for_rows(
    event_kinds: Sequence[str], view_names: Sequence[str]
) -> dict[str, str]:
    """Build commit metadata for persisted plan-artifact rows.

    Returns:
        dict[str, str]: Commit metadata map for plan artifact writes.
    """
    metadata: dict[str, str] = {
        "codeanatomy_operation": "plan_artifacts_store",
        "codeanatomy_mode": "append",
        "codeanatomy_row_count": str(len(event_kinds)),
        "codeanatomy_event_kinds": ",".join(sorted(set(event_kinds))),
    }
    unique_views = sorted(set(view_names))
    if unique_views:
        metadata["codeanatomy_first_view_name"] = unique_views[0]
        metadata["codeanatomy_view_count"] = str(len(unique_views))
    return metadata


def commit_metadata_for_pipeline_events(rows: Sequence[PipelineEventRowLike]) -> dict[str, str]:
    """Build commit metadata for persisted pipeline-event rows.

    Returns:
        dict[str, str]: Commit metadata map for pipeline event writes.
    """
    event_names = sorted({row.event_name for row in rows})
    run_ids = sorted({row.run_id for row in rows})
    metadata: dict[str, str] = {
        "codeanatomy_operation": "pipeline_events_store",
        "codeanatomy_mode": "append",
        "codeanatomy_row_count": str(len(rows)),
        "codeanatomy_event_name_count": str(len(event_names)),
    }
    if event_names:
        metadata["codeanatomy_first_event_name"] = event_names[0]
    if run_ids:
        metadata["codeanatomy_first_run_id"] = run_ids[0]
        metadata["codeanatomy_run_id_count"] = str(len(run_ids))
    return metadata


def normalized_event_payload(payload: object) -> object:
    """Normalize event payload values into mapping-compatible builtins.

    Returns:
        object: Normalized payload.
    """
    if isinstance(payload, Mapping):
        try:
            return msgspec.convert(payload, type=dict[str, object], strict=False, str_keys=True)
        except msgspec.ValidationError:
            return {str(key): value for key, value in payload.items()}
    return payload


def event_time_unix_ms(payload: object) -> int:
    """Extract event timestamp in Unix milliseconds with current-time fallback.

    Returns:
        int: Event timestamp in Unix milliseconds.
    """
    if isinstance(payload, Mapping):
        value = payload.get("event_time_unix_ms")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return int(time.time() * 1000)


def event_payload_msgpack(payload: object) -> bytes:
    """Encode event payload as msgpack bytes.

    Returns:
        bytes: Msgpack-encoded event payload.
    """
    if isinstance(payload, msgspec.Struct):
        return dumps_msgpack(payload)
    if isinstance(payload, Mapping):
        return dumps_msgpack(to_builtins(payload, str_keys=True))
    return dumps_msgpack(to_builtins(payload, str_keys=True))


def lineage_payload(report: LineageReport) -> dict[str, object]:
    """Serialize lineage report content to deterministic builtins.

    Returns:
        dict[str, object]: Serialized lineage payload.
    """
    scans = [
        {
            "dataset_name": scan.dataset_name,
            "projected_columns": list(scan.projected_columns),
            "pushed_filters": list(scan.pushed_filters),
        }
        for scan in report.scans
    ]
    joins = [
        {
            "join_type": join.join_type,
            "left_keys": list(join.left_keys),
            "right_keys": list(join.right_keys),
        }
        for join in report.joins
    ]
    exprs = [
        {
            "kind": expr.kind,
            "referenced_columns": [list(pair) for pair in expr.referenced_columns],
            "referenced_udfs": list(expr.referenced_udfs),
            "text": expr.text,
        }
        for expr in report.exprs
    ]
    required_columns = {
        dataset: list(columns) for dataset, columns in report.required_columns_by_dataset.items()
    }
    return {
        "scans": scans,
        "joins": joins,
        "exprs": exprs,
        "required_udfs": list(report.required_udfs),
        "required_rewrite_tags": list(report.required_rewrite_tags),
        "required_columns_by_dataset": required_columns,
        "filters": list(report.filters),
        "aggregations": list(report.aggregations),
        "window_functions": list(report.window_functions),
        "subqueries": list(report.subqueries),
        "referenced_udfs": list(report.referenced_udfs),
        "referenced_tables": list(report.referenced_tables),
        "all_required_columns": [list(pair) for pair in report.all_required_columns],
    }


def scan_units_payload(
    scan_units: Sequence[ScanUnit],
    *,
    scan_keys: Sequence[str],
) -> tuple[dict[str, object], ...]:
    """Serialize scan unit metadata filtered to selected scan keys.

    Returns:
        tuple[dict[str, object], ...]: Canonicalized scan-unit payloads.
    """
    scan_key_set = set(scan_keys)
    payloads: list[dict[str, object]] = []
    for unit in scan_units:
        if scan_key_set and unit.key not in scan_key_set:
            continue
        payloads.append(
            {
                "key": unit.key,
                "dataset_name": unit.dataset_name,
                "delta_version": unit.delta_version,
                "delta_timestamp": unit.delta_timestamp,
                "snapshot_timestamp": unit.snapshot_timestamp,
                "delta_protocol": (
                    to_builtins(unit.delta_protocol) if unit.delta_protocol is not None else None
                ),
                "delta_scan_config": (
                    to_builtins(unit.delta_scan_config)
                    if unit.delta_scan_config is not None
                    else None
                ),
                "delta_scan_config_hash": unit.delta_scan_config_hash,
                "datafusion_provider": unit.datafusion_provider,
                "protocol_compatible": unit.protocol_compatible,
                "protocol_compatibility": (
                    to_builtins(unit.protocol_compatibility)
                    if unit.protocol_compatibility is not None
                    else None
                ),
                "total_files": unit.total_files,
                "candidate_file_count": unit.candidate_file_count,
                "pruned_file_count": unit.pruned_file_count,
                "candidate_files": [str(path) for path in unit.candidate_files],
                "pushed_filters": list(unit.pushed_filters),
                "projected_columns": list(unit.projected_columns),
            }
        )
    payloads.sort(key=lambda item: str(item["key"]))
    return tuple(payloads)


def delta_inputs_payload(bundle: DataFusionPlanArtifact) -> tuple[dict[str, object], ...]:
    """Serialize pinned Delta inputs from a plan bundle.

    Returns:
        tuple[dict[str, object], ...]: Serialized Delta input payload rows.
    """
    payloads: list[dict[str, object]] = [
        {
            "dataset_name": pin.dataset_name,
            "version": pin.version,
            "timestamp": pin.timestamp,
            "protocol": (
                to_builtins(pin.protocol, str_keys=True) if pin.protocol is not None else None
            ),
            "delta_scan_config": (
                to_builtins(pin.delta_scan_config) if pin.delta_scan_config is not None else None
            ),
            "delta_scan_config_hash": pin.delta_scan_config_hash,
            "datafusion_provider": pin.datafusion_provider,
            "protocol_compatible": pin.protocol_compatible,
            "protocol_compatibility": (
                to_builtins(pin.protocol_compatibility)
                if pin.protocol_compatibility is not None
                else None
            ),
        }
        for pin in bundle.delta_inputs
    ]
    payloads.sort(key=lambda item: str(item["dataset_name"]))
    return tuple(payloads)


def delta_protocol_payload(protocol: object | None) -> dict[str, object] | None:
    """Normalize Delta protocol payload to a string-keyed mapping.

    Returns:
        dict[str, object] | None: Normalized protocol payload.
    """
    if protocol is None:
        return None
    if isinstance(protocol, Mapping):
        payload = {str(key): value for key, value in protocol.items()}
        return payload or None
    if isinstance(protocol, msgspec.Struct):
        resolved = to_builtins(protocol, str_keys=True)
        if isinstance(resolved, Mapping):
            return {str(key): value for key, value in resolved.items()} or None
    return None


def plan_identity_payload(
    *,
    bundle: DataFusionPlanArtifact,
    profile: DataFusionRuntimeProfile,
    delta_inputs_payload: Sequence[Mapping[str, object]],
    scan_payload: Sequence[Mapping[str, object]],
    scan_keys_payload: Sequence[str],
) -> Mapping[str, object]:
    """Build canonical plan-identity payload used for stable plan hashing.

    Returns:
        Mapping[str, object]: Stable plan identity payload.
    """
    df_settings_entries = tuple(
        sorted((str(key), str(value)) for key, value in bundle.artifacts.df_settings.items())
    )
    return {
        "version": 4,
        "plan_fingerprint": bundle.plan_fingerprint,
        "udf_snapshot_hash": bundle.artifacts.udf_snapshot_hash,
        "function_registry_hash": bundle.artifacts.function_registry_hash,
        "required_udfs": tuple(sorted(bundle.required_udfs)),
        "required_rewrite_tags": tuple(sorted(bundle.required_rewrite_tags)),
        "domain_planner_names": tuple(sorted(bundle.artifacts.domain_planner_names)),
        "df_settings_entries": df_settings_entries,
        "planning_env_hash": bundle.artifacts.planning_env_hash,
        "rulepack_hash": bundle.artifacts.rulepack_hash,
        "information_schema_hash": bundle.artifacts.information_schema_hash,
        "delta_inputs": tuple(delta_inputs_payload),
        "scan_units": tuple(scan_payload),
        "scan_keys": tuple(sorted(set(scan_keys_payload))),
        "profile_settings_hash": profile.settings_hash(),
        "profile_context_key": profile.context_cache_key(),
    }


def plan_details_payload(
    bundle: DataFusionPlanArtifact,
    *,
    delta_inputs_payload: Sequence[Mapping[str, object]],
    scan_payload: Sequence[Mapping[str, object]],
    plan_identity_hash: str,
) -> Mapping[str, object]:
    """Build plan details payload augmented with deterministic hash fields.

    Returns:
        Mapping[str, object]: Enriched plan details payload.
    """
    base_details = dict(bundle.plan_details)
    base_details["delta_inputs_hash"] = hash_json_default(delta_inputs_payload)
    base_details["scan_units_hash"] = hash_json_default(scan_payload)
    base_details["df_settings_hash"] = hash_json_default(bundle.artifacts.df_settings)
    base_details["plan_identity_hash"] = plan_identity_hash
    return base_details


def udf_compatibility(
    ctx: SessionContext,
    bundle: DataFusionPlanArtifact,
) -> tuple[bool, Mapping[str, object]]:
    """Evaluate execution-time UDF snapshot compatibility for a planned bundle.

    Returns:
        tuple[bool, Mapping[str, object]]: Compatibility status and diagnostic detail.
    """
    from datafusion_engine.udf.extension_core import (
        rust_udf_snapshot,
        rust_udf_snapshot_hash,
        udf_names_from_snapshot,
    )

    snapshot = rust_udf_snapshot(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    planned_hash = bundle.artifacts.udf_snapshot_hash
    snapshot_match = snapshot_hash == planned_hash
    snapshot_udfs = set(udf_names_from_snapshot(snapshot))
    missing_udfs = sorted(set(bundle.required_udfs) - snapshot_udfs)
    compatibility_ok = snapshot_match and not missing_udfs
    detail = {
        "planned_snapshot_hash": planned_hash,
        "execution_snapshot_hash": snapshot_hash,
        "snapshot_match": snapshot_match,
        "required_udf_count": len(bundle.required_udfs),
        "snapshot_udf_count": len(snapshot_udfs),
        "missing_udfs": missing_udfs,
    }
    return compatibility_ok, detail


def substrait_msgpack(payload: bytes) -> bytes:
    """Encode Substrait bytes payload as msgpack.

    Returns:
        bytes: Msgpack-encoded Substrait payload.
    """
    return dumps_msgpack(SubstraitBytes(payload))


def proto_msgpack(payload: object | None) -> bytes | None:
    """Encode optional protobuf payload as msgpack bytes.

    Returns:
        bytes | None: Msgpack-encoded protobuf payload or ``None``.
    """
    if payload is None:
        return None
    return dumps_msgpack(payload)


def msgpack_payload(payload: object) -> bytes:
    """Encode payload as msgpack after deterministic builtins conversion.

    Returns:
        bytes: Msgpack-encoded payload.
    """
    return dumps_msgpack(to_builtins(payload, str_keys=True))


def msgpack_payload_raw(payload: object) -> msgspec.Raw:
    """Encode payload as ``msgspec.Raw`` msgpack bytes.

    Returns:
        msgspec.Raw: Raw msgpack payload.
    """
    return ensure_raw(dumps_msgpack(to_builtins(payload, str_keys=True)))


def msgpack_or_none(payload: object | None) -> bytes | None:
    """Encode payload as msgpack or return ``None`` when payload is missing.

    Returns:
        bytes | None: Msgpack-encoded payload or ``None``.
    """
    if payload is None:
        return None
    return dumps_msgpack(to_builtins(payload, str_keys=True))


__all__ = [
    "bundle_payload",
    "commit_metadata_for_pipeline_events",
    "commit_metadata_for_rows",
    "delta_inputs_payload",
    "delta_protocol_payload",
    "event_payload_msgpack",
    "event_time_unix_ms",
    "lineage_payload",
    "msgpack_or_none",
    "msgpack_payload",
    "msgpack_payload_raw",
    "normalized_event_payload",
    "plan_details_payload",
    "plan_identity_payload",
    "proto_msgpack",
    "scan_units_payload",
    "substrait_msgpack",
    "udf_compatibility",
]
