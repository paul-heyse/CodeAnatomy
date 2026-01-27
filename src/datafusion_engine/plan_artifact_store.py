"""Delta-backed plan artifact persistence for deterministic observability."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.dataset_registry import (
    DatasetLocation,
    resolve_delta_log_storage_options,
    resolve_delta_scan_options,
)
from datafusion_engine.diagnostics import record_artifact
from serde_msgspec import dumps_json, to_builtins
from storage.deltalake import (
    DeltaWriteOptions,
    delta_table_version,
    enable_delta_features,
    idempotent_commit_properties,
    write_delta_table,
)
from storage.deltalake.delta import DEFAULT_DELTA_FEATURE_PROPERTIES

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.lineage_datafusion import LineageReport
    from datafusion_engine.plan_bundle import DataFusionPlanBundle
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.scan_planner import ScanUnit
    from datafusion_engine.view_graph_registry import ViewNode

PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v1"
WRITE_ARTIFACTS_TABLE_NAME = "datafusion_write_artifacts_v1"
_ARTIFACTS_DIRNAME = PLAN_ARTIFACTS_TABLE_NAME
_WRITE_ARTIFACTS_DIRNAME = WRITE_ARTIFACTS_TABLE_NAME
_LOCAL_ARTIFACTS_DIRNAME = "artifacts"
_PLAN_EVENT_KIND = "plan"
_EXECUTION_EVENT_KIND = "execution"
_WRITE_EVENT_KIND = "write"
try:
    _DEFAULT_ARTIFACTS_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_ARTIFACTS_ROOT = Path.cwd() / ".artifacts"


@dataclass(frozen=True)
class PlanArtifactRow:
    """Serializable plan artifact row persisted to the Delta store."""

    event_time_unix_ms: int
    profile_name: str | None
    event_kind: str
    view_name: str
    plan_fingerprint: str
    udf_snapshot_hash: str
    function_registry_hash: str
    required_udfs_json: str
    required_rewrite_tags_json: str
    domain_planner_names_json: str
    delta_inputs_json: str
    df_settings_json: str
    substrait_b64: str | None
    logical_plan_display: str | None
    optimized_plan_display: str | None
    optimized_plan_pgjson: str | None
    optimized_plan_graphviz: str | None
    execution_plan_display: str | None
    lineage_json: str
    scan_units_json: str
    scan_keys_json: str
    plan_details_json: str
    function_registry_snapshot_json: str
    udf_snapshot_json: str
    udf_compatibility_ok: bool
    udf_compatibility_detail_json: str
    execution_duration_ms: float | None = None
    execution_status: str | None = None
    execution_error: str | None = None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready row mapping.

        Returns
        -------
        dict[str, object]
            JSON-ready row payload.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms,
            "profile_name": self.profile_name,
            "event_kind": self.event_kind,
            "view_name": self.view_name,
            "plan_fingerprint": self.plan_fingerprint,
            "udf_snapshot_hash": self.udf_snapshot_hash,
            "function_registry_hash": self.function_registry_hash,
            "required_udfs_json": self.required_udfs_json,
            "required_rewrite_tags_json": self.required_rewrite_tags_json,
            "domain_planner_names_json": self.domain_planner_names_json,
            "delta_inputs_json": self.delta_inputs_json,
            "df_settings_json": self.df_settings_json,
            "substrait_b64": self.substrait_b64,
            "logical_plan_display": self.logical_plan_display,
            "optimized_plan_display": self.optimized_plan_display,
            "optimized_plan_pgjson": self.optimized_plan_pgjson,
            "optimized_plan_graphviz": self.optimized_plan_graphviz,
            "execution_plan_display": self.execution_plan_display,
            "lineage_json": self.lineage_json,
            "scan_units_json": self.scan_units_json,
            "scan_keys_json": self.scan_keys_json,
            "plan_details_json": self.plan_details_json,
            "function_registry_snapshot_json": self.function_registry_snapshot_json,
            "udf_snapshot_json": self.udf_snapshot_json,
            "udf_compatibility_ok": self.udf_compatibility_ok,
            "udf_compatibility_detail_json": self.udf_compatibility_detail_json,
            "execution_duration_ms": self.execution_duration_ms,
            "execution_status": self.execution_status,
            "execution_error": self.execution_error,
        }


@dataclass(frozen=True)
class DeterminismValidationResult:
    """Result of plan determinism validation against artifact store."""

    is_deterministic: bool
    plan_fingerprint: str
    view_name: str | None
    matching_artifact_count: int
    conflicting_fingerprints: tuple[str, ...]
    validation_error: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for determinism validation.

        Returns
        -------
        dict[str, object]
            Serializable diagnostics payload.
        """
        return {
            "is_deterministic": self.is_deterministic,
            "plan_fingerprint": self.plan_fingerprint,
            "view_name": self.view_name,
            "matching_artifact_count": self.matching_artifact_count,
            "conflicting_fingerprints": list(self.conflicting_fingerprints),
            "validation_error": self.validation_error,
        }


@dataclass(frozen=True)
class WriteArtifactRow:
    """Serializable write artifact row persisted to the Delta store."""

    event_time_unix_ms: int
    profile_name: str | None
    event_kind: str
    destination: str
    format: str
    mode: str
    method: str
    table_uri: str
    delta_version: int | None
    commit_app_id: str | None
    commit_version: int | None
    commit_run_id: str | None
    partition_by_json: str
    table_properties_json: str
    commit_metadata_json: str
    duration_ms: float | None
    row_count: int | None
    status: str | None
    error: str | None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready row mapping.

        Returns
        -------
        dict[str, object]
            JSON-ready row payload.
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
            "partition_by_json": self.partition_by_json,
            "table_properties_json": self.table_properties_json,
            "commit_metadata_json": self.commit_metadata_json,
            "duration_ms": self.duration_ms,
            "row_count": self.row_count,
            "status": self.status,
            "error": self.error,
        }


def ensure_plan_artifacts_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure the plan artifacts table exists and is registered.

    Returns
    -------
    DatasetLocation | None
        Location for the plan artifacts table when enabled.
    """
    location = _plan_artifacts_location(profile)
    if location is None:
        return None
    table_path = Path(location.path)
    existing_version = delta_table_version(str(table_path))
    if existing_version is None:
        _bootstrap_plan_artifacts_table(profile, table_path)
    _refresh_plan_artifacts_registration(ctx, profile, location)
    return location


@dataclass(frozen=True)
class PlanArtifactsForViewsRequest:
    """Inputs for persisting plan artifacts for view nodes."""

    view_nodes: Sequence[ViewNode]
    scan_units: Sequence[ScanUnit] = ()
    scan_keys_by_view: Mapping[str, Sequence[str]] | None = None
    lineage_by_view: Mapping[str, LineageReport] | None = None


def persist_plan_artifacts_for_views(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactsForViewsRequest,
) -> tuple[PlanArtifactRow, ...]:
    """Persist plan artifacts for view nodes with plan bundles.

    Parameters
    ----------
    request
        Plan artifact persistence request payload.

    Returns
    -------
    tuple[PlanArtifactRow, ...]
        Persisted plan artifact rows.
    """
    location = ensure_plan_artifacts_table(ctx, profile)
    if location is None:
        return ()
    rows: list[PlanArtifactRow] = []
    for node in request.view_nodes:
        bundle = node.plan_bundle
        if bundle is None:
            continue
        scan_keys = (
            tuple(request.scan_keys_by_view.get(node.name, ()))
            if request.scan_keys_by_view
            else ()
        )
        lineage = request.lineage_by_view.get(node.name) if request.lineage_by_view else None
        rows.append(
            build_plan_artifact_row(
                ctx,
                profile,
                request=PlanArtifactBuildRequest(
                    view_name=node.name,
                    bundle=bundle,
                    lineage=lineage,
                    scan_units=request.scan_units,
                    scan_keys=scan_keys,
                ),
            )
        )
    if not rows:
        return ()
    return persist_plan_artifact_rows(ctx, profile, rows=rows, location=location)


def validate_plan_determinism(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    plan_fingerprint: str,
    view_name: str | None = None,
) -> DeterminismValidationResult:
    """Validate that a plan fingerprint is deterministic via artifact queries.

    Queries the plan artifact table to check if the same logical plan
    consistently produces the same fingerprint across executions.

    Parameters
    ----------
    ctx
        DataFusion SessionContext for artifact queries.
    profile
        Runtime profile for artifact table configuration.
    plan_fingerprint
        Plan fingerprint to validate.
    view_name
        Optional view name to scope the validation query.

    Returns
    -------
    DeterminismValidationResult
        Result indicating whether the plan is deterministic.
    """
    location = _plan_artifacts_location(profile)
    if location is None:
        return DeterminismValidationResult(
            is_deterministic=True,
            plan_fingerprint=plan_fingerprint,
            view_name=view_name,
            matching_artifact_count=0,
            conflicting_fingerprints=(),
            validation_error="artifact_store_disabled",
        )
    try:
        table_path = str(location.path)
        query = _determinism_validation_query(table_path, view_name=view_name)
        result_df = ctx.sql(query)
        results = result_df.collect()
    except (RuntimeError, ValueError, TypeError) as exc:
        return DeterminismValidationResult(
            is_deterministic=True,
            plan_fingerprint=plan_fingerprint,
            view_name=view_name,
            matching_artifact_count=0,
            conflicting_fingerprints=(),
            validation_error=str(exc),
        )
    fingerprints: set[str] = set()
    row_count = 0
    for batch in results:
        for row in batch.to_pylist():
            row_count += 1
            fp = row.get("plan_fingerprint")
            if fp is not None:
                fingerprints.add(str(fp))
    is_deterministic = len(fingerprints) <= 1 or plan_fingerprint in fingerprints
    conflicting = tuple(sorted(fp for fp in fingerprints if fp != plan_fingerprint))
    return DeterminismValidationResult(
        is_deterministic=is_deterministic,
        plan_fingerprint=plan_fingerprint,
        view_name=view_name,
        matching_artifact_count=row_count,
        conflicting_fingerprints=conflicting,
    )


def _determinism_validation_query(
    table_path: str,
    *,
    view_name: str | None,
) -> str:
    """Build SQL query for determinism validation.

    Returns
    -------
    str
        SQL query for determinism checks.
    """
    base = f"SELECT DISTINCT plan_fingerprint FROM delta_scan('{table_path}')"
    if view_name is not None:
        return f"{base} WHERE view_name = '{view_name}'"
    return base


@dataclass(frozen=True)
class PlanArtifactBuildRequest:
    """Inputs for building plan artifact rows."""

    view_name: str
    bundle: DataFusionPlanBundle
    lineage: LineageReport | None = None
    scan_units: Sequence[ScanUnit] = ()
    scan_keys: Sequence[str] = ()
    event_kind: str = _PLAN_EVENT_KIND
    execution_duration_ms: float | None = None
    execution_status: str | None = None
    execution_error: str | None = None


def persist_execution_artifact(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactBuildRequest,
) -> PlanArtifactRow | None:
    """Persist an execution artifact row for a plan bundle.

    Returns
    -------
    PlanArtifactRow | None
        Persisted plan artifact row when storage is enabled.
    """
    location = ensure_plan_artifacts_table(ctx, profile)
    if location is None:
        return None
    row = build_plan_artifact_row(
        ctx,
        profile,
        request=replace(request, event_kind=_EXECUTION_EVENT_KIND),
    )
    persisted = persist_plan_artifact_rows(ctx, profile, rows=(row,), location=location)
    return persisted[0] if persisted else None


@dataclass(frozen=True)
class WriteArtifactRequest:
    """Inputs for persisting write artifacts."""

    destination: str
    write_format: str
    mode: str
    method: str
    table_uri: str
    delta_version: int | None = None
    commit_app_id: str | None = None
    commit_version: int | None = None
    commit_run_id: str | None = None
    partition_by: Sequence[str] = ()
    table_properties: Mapping[str, str] | None = None
    commit_metadata: Mapping[str, str] | None = None
    duration_ms: float | None = None
    row_count: int | None = None
    status: str | None = None
    error: str | None = None


def persist_write_artifact(
    profile: DataFusionRuntimeProfile,
    *,
    request: WriteArtifactRequest,
) -> WriteArtifactRow | None:
    """Persist a write artifact row for a Delta write operation.

    Parameters
    ----------
    profile
        Runtime profile for artifact storage configuration.
    request
        Write artifact request payload.

    Returns
    -------
    WriteArtifactRow | None
        Persisted write artifact row, or None if persistence is disabled.
    """
    row = WriteArtifactRow(
        event_time_unix_ms=int(time.time() * 1000),
        profile_name=_profile_name(profile),
        event_kind=_WRITE_EVENT_KIND,
        destination=request.destination,
        format=request.write_format,
        mode=request.mode,
        method=request.method,
        table_uri=request.table_uri,
        delta_version=request.delta_version,
        commit_app_id=request.commit_app_id,
        commit_version=request.commit_version,
        commit_run_id=request.commit_run_id,
        partition_by_json=_json_text(list(request.partition_by)),
        table_properties_json=_json_text(
            dict(request.table_properties) if request.table_properties else {}
        ),
        commit_metadata_json=_json_text(
            dict(request.commit_metadata) if request.commit_metadata else {}
        ),
        duration_ms=request.duration_ms,
        row_count=request.row_count,
        status=request.status,
        error=request.error,
    )
    record_artifact(profile, "write_artifact_v1", row.to_row())
    return row


def persist_plan_artifact_rows(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PlanArtifactRow],
    location: DatasetLocation | None = None,
) -> tuple[PlanArtifactRow, ...]:
    """Persist plan artifact rows to the Delta-backed artifact store.

    Returns
    -------
    tuple[PlanArtifactRow, ...]
        Persisted plan artifact rows.

    Raises
    ------
    RuntimeError
        Raised when the Delta version cannot be resolved after write.
    """
    if not rows:
        return ()
    resolved_location = location or ensure_plan_artifacts_table(ctx, profile)
    if resolved_location is None:
        return ()
    table_path = Path(resolved_location.path)
    arrow_table = pa.Table.from_pylist(
        [row.to_row() for row in rows],
        schema=_plan_artifacts_schema(),
    )
    commit_metadata = _commit_metadata_for_rows(rows)
    commit_key = str(table_path)
    commit_options, commit_run = profile.reserve_delta_commit(
        key=commit_key,
        metadata=commit_metadata,
        commit_metadata=commit_metadata,
    )
    commit_properties = idempotent_commit_properties(
        operation="plan_artifacts_store",
        mode="append",
        idempotent=commit_options,
        extra_metadata=commit_metadata,
    )
    options = DeltaWriteOptions(
        mode="append",
        schema_mode="merge",
        configuration=DEFAULT_DELTA_FEATURE_PROPERTIES,
        commit_metadata=commit_metadata,
        commit_properties=commit_properties,
    )
    write_result = write_delta_table(
        arrow_table,
        str(table_path),
        options=options,
    )
    final_version = delta_table_version(str(table_path))
    if final_version is None:
        final_version = write_result.version
    if final_version is None:
        msg = f"Failed to resolve Delta version for plan artifacts: {table_path}."
        raise RuntimeError(msg)
    profile.finalize_delta_commit(
        key=commit_key,
        run=commit_run,
        metadata={
            "operation": "plan_artifacts_store",
            "row_count": len(rows),
            "delta_version": final_version,
        },
    )
    _refresh_plan_artifacts_registration(ctx, profile, resolved_location)
    _record_plan_artifact_summary(profile, rows=rows, path=str(table_path), version=final_version)
    return tuple(rows)


def build_plan_artifact_row(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    request: PlanArtifactBuildRequest,
) -> PlanArtifactRow:
    """Build a plan artifact row from a plan bundle and optional lineage.

    Returns
    -------
    PlanArtifactRow
        Serialized plan artifact row.
    """
    resolved_lineage = request.lineage or _lineage_from_bundle(request.bundle)
    scan_payload = _scan_units_payload(request.scan_units, scan_keys=request.scan_keys)
    scan_keys_payload = tuple(sorted(set(request.scan_keys)))
    delta_inputs_payload = _delta_inputs_payload(request.bundle)
    udf_ok, udf_detail = _udf_compatibility(ctx, request.bundle)
    plan_details_payload = _plan_details_payload(
        request.bundle,
        delta_inputs_payload=delta_inputs_payload,
        scan_payload=scan_payload,
    )
    return PlanArtifactRow(
        event_time_unix_ms=int(time.time() * 1000),
        profile_name=_profile_name(profile),
        event_kind=request.event_kind,
        view_name=request.view_name,
        plan_fingerprint=request.bundle.plan_fingerprint,
        udf_snapshot_hash=request.bundle.artifacts.udf_snapshot_hash,
        function_registry_hash=request.bundle.artifacts.function_registry_hash,
        required_udfs_json=_json_text(request.bundle.required_udfs),
        required_rewrite_tags_json=_json_text(request.bundle.required_rewrite_tags),
        domain_planner_names_json=_json_text(request.bundle.artifacts.domain_planner_names),
        delta_inputs_json=_json_text(delta_inputs_payload),
        df_settings_json=_json_text(request.bundle.artifacts.df_settings),
        substrait_b64=_substrait_b64(request.bundle.substrait_bytes),
        logical_plan_display=request.bundle.artifacts.logical_plan_display,
        optimized_plan_display=request.bundle.artifacts.optimized_plan_display,
        optimized_plan_pgjson=request.bundle.artifacts.optimized_plan_pgjson,
        optimized_plan_graphviz=request.bundle.artifacts.optimized_plan_graphviz,
        execution_plan_display=request.bundle.artifacts.execution_plan_display,
        lineage_json=_json_text(_lineage_payload(resolved_lineage)),
        scan_units_json=_json_text(scan_payload),
        scan_keys_json=_json_text(scan_keys_payload),
        plan_details_json=_json_text(plan_details_payload),
        function_registry_snapshot_json=_json_text(
            request.bundle.artifacts.function_registry_snapshot
        ),
        udf_snapshot_json=_json_text(request.bundle.artifacts.udf_snapshot),
        udf_compatibility_ok=udf_ok,
        udf_compatibility_detail_json=_json_text(udf_detail),
        execution_duration_ms=request.execution_duration_ms,
        execution_status=request.execution_status,
        execution_error=request.execution_error,
    )


def _plan_artifacts_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    root = _plan_artifacts_root(profile)
    if root is None:
        return None
    location = DatasetLocation(
        path=str(root / _ARTIFACTS_DIRNAME),
        format="delta",
        storage_options={},
        delta_log_storage_options={},
    )
    return _with_delta_settings(location)


def _with_delta_settings(location: DatasetLocation) -> DatasetLocation:
    resolved_scan = resolve_delta_scan_options(location)
    resolved_log = resolve_delta_log_storage_options(location)
    return replace(
        location,
        delta_scan=resolved_scan,
        delta_log_storage_options=dict(resolved_log or {}),
    )


def _plan_artifacts_root(profile: DataFusionRuntimeProfile) -> Path | None:
    root_value = profile.plan_artifacts_root
    if root_value is None:
        if profile.local_filesystem_root is not None:
            root_value = str(Path(profile.local_filesystem_root) / _LOCAL_ARTIFACTS_DIRNAME)
        else:
            root_value = str(_DEFAULT_ARTIFACTS_ROOT)
    if "://" in root_value:
        record_artifact(
            profile,
            "plan_artifacts_store_unavailable_v1",
            {
                "reason": "non_local_root",
                "root": root_value,
            },
        )
        return None
    root_path = Path(root_value)
    root_path.mkdir(parents=True, exist_ok=True)
    return root_path


def _profile_name(profile: DataFusionRuntimeProfile) -> str | None:
    return profile.config_policy_name


def _plan_artifacts_schema() -> pa.Schema:
    from datafusion_engine.schema_registry import DATAFUSION_PLAN_ARTIFACTS_SCHEMA

    schema = DATAFUSION_PLAN_ARTIFACTS_SCHEMA
    if isinstance(schema, pa.Schema):
        return schema
    return pa.schema(schema)


def _bootstrap_plan_artifacts_table(
    _profile: DataFusionRuntimeProfile,
    table_path: Path,
) -> None:
    schema = _plan_artifacts_schema()
    empty_table = pa.Table.from_pylist([], schema=schema)
    commit_metadata = {
        "operation": "plan_artifacts_bootstrap",
        "mode": "overwrite",
        "table": PLAN_ARTIFACTS_TABLE_NAME,
    }
    commit_properties = idempotent_commit_properties(
        operation="plan_artifacts_bootstrap",
        mode="overwrite",
        extra_metadata=commit_metadata,
    )
    options = DeltaWriteOptions(
        mode="overwrite",
        schema_mode="overwrite",
        configuration=DEFAULT_DELTA_FEATURE_PROPERTIES,
        commit_metadata=commit_metadata,
        commit_properties=commit_properties,
    )
    write_delta_table(empty_table, str(table_path), options=options)
    enable_delta_features(
        str(table_path),
        features=DEFAULT_DELTA_FEATURE_PROPERTIES,
        commit_metadata=commit_metadata,
    )


def _refresh_plan_artifacts_registration(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    location: DatasetLocation,
) -> None:
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from datafusion_engine.io_adapter import DataFusionIOAdapter
    from datafusion_engine.registry_bridge import DataFusionCachePolicy

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(PLAN_ARTIFACTS_TABLE_NAME):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(PLAN_ARTIFACTS_TABLE_NAME)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(
        name=PLAN_ARTIFACTS_TABLE_NAME,
        location=location,
        cache_policy=DataFusionCachePolicy(enabled=False, max_columns=None),
    )


def _record_plan_artifact_summary(
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PlanArtifactRow],
    path: str,
    version: int,
) -> None:
    kinds = sorted({row.event_kind for row in rows})
    payload = {
        "table": PLAN_ARTIFACTS_TABLE_NAME,
        "path": path,
        "row_count": len(rows),
        "event_kinds": kinds,
        "view_names": sorted({row.view_name for row in rows}),
        "delta_version": version,
    }
    record_artifact(profile, "plan_artifacts_store_v1", payload)
    for row in rows:
        record_artifact(profile, PLAN_ARTIFACTS_TABLE_NAME, row.to_row())


def _commit_metadata_for_rows(rows: Sequence[PlanArtifactRow]) -> dict[str, str]:
    event_kinds = sorted({row.event_kind for row in rows})
    view_names = sorted({row.view_name for row in rows})
    metadata: dict[str, str] = {
        "operation": "plan_artifacts_store",
        "mode": "append",
        "row_count": str(len(rows)),
        "event_kinds": ",".join(event_kinds),
    }
    if view_names:
        metadata["first_view_name"] = view_names[0]
        metadata["view_count"] = str(len(view_names))
    return metadata


def _lineage_from_bundle(bundle: DataFusionPlanBundle) -> LineageReport:
    from datafusion_engine.lineage_datafusion import extract_lineage

    return extract_lineage(
        bundle.optimized_logical_plan,
        udf_snapshot=bundle.artifacts.udf_snapshot,
    )


def _lineage_payload(report: LineageReport) -> dict[str, object]:
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


def _scan_units_payload(
    scan_units: Sequence[ScanUnit],
    *,
    scan_keys: Sequence[str],
) -> tuple[dict[str, object], ...]:
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
                "candidate_files": [str(path) for path in unit.candidate_files],
                "pushed_filters": list(unit.pushed_filters),
                "projected_columns": list(unit.projected_columns),
            }
        )
    payloads.sort(key=lambda item: str(item["key"]))
    return tuple(payloads)


def _delta_inputs_payload(bundle: DataFusionPlanBundle) -> tuple[dict[str, object], ...]:
    payloads = [
        {
            "dataset_name": pin.dataset_name,
            "version": pin.version,
            "timestamp": pin.timestamp,
        }
        for pin in bundle.delta_inputs
    ]
    payloads.sort(key=lambda item: str(item["dataset_name"]))
    return tuple(payloads)


def _plan_details_payload(
    bundle: DataFusionPlanBundle,
    *,
    delta_inputs_payload: Sequence[Mapping[str, object]],
    scan_payload: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    base_details = dict(bundle.plan_details)
    base_details["delta_inputs_hash"] = _payload_hash(delta_inputs_payload)
    base_details["scan_units_hash"] = _payload_hash(scan_payload)
    base_details["df_settings_hash"] = _payload_hash(bundle.artifacts.df_settings)
    return base_details


def _payload_hash(payload: object) -> str:
    raw = dumps_json(to_builtins(payload))
    return hashlib.sha256(raw).hexdigest()


def _udf_compatibility(
    ctx: SessionContext,
    bundle: DataFusionPlanBundle,
) -> tuple[bool, Mapping[str, object]]:
    from datafusion_engine.udf_runtime import (
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


def _substrait_b64(payload: bytes | None) -> str | None:
    if payload is None:
        return None
    return base64.b64encode(payload).decode("ascii")


def _json_text(payload: object) -> str:
    try:
        raw = dumps_json(to_builtins(payload))
        return raw.decode("utf-8")
    except (TypeError, ValueError) as exc:
        fallback = {
            "error": type(exc).__name__,
            "repr": repr(payload),
        }
        return dumps_json(fallback).decode("utf-8")


__all__ = [
    "PLAN_ARTIFACTS_TABLE_NAME",
    "WRITE_ARTIFACTS_TABLE_NAME",
    "DeterminismValidationResult",
    "PlanArtifactRow",
    "WriteArtifactRow",
    "build_plan_artifact_row",
    "ensure_plan_artifacts_table",
    "persist_execution_artifact",
    "persist_plan_artifact_rows",
    "persist_plan_artifacts_for_views",
    "persist_write_artifact",
    "validate_plan_determinism",
]
