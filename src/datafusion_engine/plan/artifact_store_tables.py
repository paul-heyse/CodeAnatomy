"""Artifact-store table location/bootstrap helpers extracted from artifact_store_core."""

from __future__ import annotations

import contextlib
import shutil
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.delta.scan_config import resolve_delta_scan_options
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.plan.artifact_store_constants import (
    _ARTIFACTS_DIRNAME,
    _DEFAULT_ARTIFACTS_ROOT,
    _LOCAL_ARTIFACTS_DIRNAME,
    _PIPELINE_EVENTS_DIRNAME,
    PIPELINE_EVENTS_TABLE_NAME,
    PLAN_ARTIFACTS_TABLE_NAME,
)
from datafusion_engine.plan.artifact_store_persistence import (
    _ArtifactTableWriteRequest,
    _write_artifact_table,
)
from schema_spec.dataset_spec import dataset_spec_from_schema
from serde_artifacts import PlanArtifactRow
from storage.deltalake import DeltaSchemaRequest

if TYPE_CHECKING:
    from datafusion_engine.plan.artifact_store_core import PipelineEventRow
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def plan_artifacts_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    """Return the canonical dataset location for plan artifact records.

    Returns:
    -------
    DatasetLocation | None
        Delta dataset location for plan artifacts, or ``None`` when unavailable.
    """
    root = plan_artifacts_root(profile)
    if root is None:
        return None
    dataset_spec = dataset_spec_from_schema(
        PLAN_ARTIFACTS_TABLE_NAME,
        plan_artifacts_schema(),
    )
    location = DatasetLocation(
        path=str(root / _ARTIFACTS_DIRNAME),
        format="delta",
        storage_options={},
        delta_log_storage_options={},
        dataset_spec=dataset_spec,
    )
    return with_delta_settings(location)


def pipeline_events_location(profile: DataFusionRuntimeProfile) -> DatasetLocation | None:
    """Return the canonical dataset location for pipeline event records.

    Returns:
    -------
    DatasetLocation | None
        Delta dataset location for pipeline events, or ``None`` when unavailable.
    """
    root = plan_artifacts_root(profile)
    if root is None:
        return None
    dataset_spec = dataset_spec_from_schema(
        PIPELINE_EVENTS_TABLE_NAME,
        pipeline_events_schema(),
    )
    location = DatasetLocation(
        path=str(root / _PIPELINE_EVENTS_DIRNAME),
        format="delta",
        storage_options={},
        delta_log_storage_options={},
        dataset_spec=dataset_spec,
    )
    return with_delta_settings(location)


def with_delta_settings(location: DatasetLocation) -> DatasetLocation:
    """Apply resolved Delta scan and log-store settings to a dataset location.

    Returns:
    -------
    DatasetLocation
        Dataset location with effective Delta overrides materialized.
    """
    resolved_scan = resolve_delta_scan_options(location)
    resolved_log = location.resolved_delta_log_storage_options
    overrides = location.overrides
    if resolved_scan is not None:
        from schema_spec.dataset_spec import DeltaPolicyBundle

        delta_bundle = DeltaPolicyBundle(scan=resolved_scan)
        if overrides is None:
            overrides = DatasetLocationOverrides(delta=delta_bundle)
        else:
            overrides = msgspec.structs.replace(overrides, delta=delta_bundle)
    return msgspec.structs.replace(
        location,
        overrides=overrides,
        delta_log_storage_options=dict(resolved_log or {}),
    )


def delta_schema_available(
    location: DatasetLocation,
    *,
    profile: DataFusionRuntimeProfile,
) -> bool:
    """Return whether a Delta table schema can be resolved through runtime services.

    Returns:
    -------
    bool
        ``True`` when schema resolution succeeds.
    """
    schema = profile.delta_ops.delta_service().table_schema(
        DeltaSchemaRequest(
            path=str(location.path),
            storage_options=location.storage_options or None,
            log_storage_options=location.delta_log_storage_options or None,
            version=location.delta_version,
            timestamp=location.delta_timestamp,
            gate=location.delta_feature_gate,
        )
    )
    return schema is not None


def reset_artifacts_table_path(
    profile: DataFusionRuntimeProfile,
    table_path: Path,
    *,
    table_name: str,
    reason: str,
) -> None:
    """Reset an artifact table directory and record the reset event."""
    if table_path.exists():
        shutil.rmtree(table_path)
    table_path.mkdir(parents=True, exist_ok=True)
    from serde_artifact_specs import ARTIFACT_STORE_RESET_SPEC

    record_artifact(
        profile,
        ARTIFACT_STORE_RESET_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": table_name,
            "path": str(table_path),
            "reason": reason,
        },
    )


def plan_artifacts_root(profile: DataFusionRuntimeProfile) -> Path | None:
    """Resolve and initialize the root directory used for local plan artifacts.

    Returns:
    -------
    pathlib.Path | None
        Local root directory, or ``None`` when storage is unavailable.
    """
    root_value = profile.policies.plan_artifacts_root
    if root_value is None:
        if profile.policies.local_filesystem_root is not None:
            root_value = str(
                Path(profile.policies.local_filesystem_root) / _LOCAL_ARTIFACTS_DIRNAME
            )
        else:
            root_value = str(_DEFAULT_ARTIFACTS_ROOT)
    if "://" in root_value:
        from serde_artifact_specs import PLAN_ARTIFACTS_STORE_UNAVAILABLE_SPEC

        record_artifact(
            profile,
            PLAN_ARTIFACTS_STORE_UNAVAILABLE_SPEC,
            {
                "reason": "non_local_root",
                "root": root_value,
            },
        )
        return None
    root_path = Path(root_value)
    root_path.mkdir(parents=True, exist_ok=True)
    return root_path


def profile_name(profile: DataFusionRuntimeProfile) -> str | None:
    """Return the configured profile name when available.

    Returns:
    -------
    str | None
        Profile name, or ``None`` when unnamed.
    """
    return profile.policies.config_policy_name


def plan_artifacts_schema() -> pa.Schema:
    """Return the Arrow schema used for the plan artifacts Delta table.

    Returns:
    -------
    pyarrow.Schema
        Normalized plan artifacts schema.
    """
    from datafusion_engine.schema import DATAFUSION_PLAN_ARTIFACTS_SCHEMA

    schema = DATAFUSION_PLAN_ARTIFACTS_SCHEMA
    if isinstance(schema, pa.Schema):
        return schema
    return pa.schema(schema)


def bootstrap_plan_artifacts_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    table_path: Path,
    *,
    schema: pa.Schema | None = None,
    table_name: str = PLAN_ARTIFACTS_TABLE_NAME,
) -> None:
    """Create or overwrite the plan artifacts table with an empty bootstrap commit."""
    resolved_schema = schema or plan_artifacts_schema()
    empty_table = pa.Table.from_pylist([], schema=resolved_schema)
    commit_metadata = {
        "codeanatomy_operation": "plan_artifacts_bootstrap",
        "codeanatomy_mode": "overwrite",
        "codeanatomy_table": table_name,
    }
    _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=empty_table,
            commit_metadata=commit_metadata,
            mode="overwrite",
            schema_mode="overwrite",
            operation_id="plan_artifacts_bootstrap",
        ),
    )


def refresh_plan_artifacts_registration(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    location: DatasetLocation,
    *,
    table_name: str = PLAN_ARTIFACTS_TABLE_NAME,
) -> None:
    """Re-register the plan artifacts table in the active DataFusion context."""
    from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(table_name):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(table_name)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(
        name=table_name,
        location=location,
        cache_policy=DataFusionCachePolicy(enabled=False, max_columns=None),
    )


def record_plan_artifact_summary(
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PlanArtifactRow],
    path: str,
    version: int,
    table_name: str = PLAN_ARTIFACTS_TABLE_NAME,
) -> None:
    """Emit summary and per-row telemetry artifacts for stored plan artifacts."""
    kinds = sorted({row.event_kind for row in rows})
    payload = {
        "table": table_name,
        "path": path,
        "row_count": len(rows),
        "event_kinds": kinds,
        "view_names": sorted({row.view_name for row in rows}),
        "delta_version": version,
    }
    from serde_artifact_specs import DATAFUSION_PLAN_ARTIFACTS_SPEC, PLAN_ARTIFACTS_STORE_SPEC

    record_artifact(profile, PLAN_ARTIFACTS_STORE_SPEC, payload)
    for row in rows:
        record_artifact(profile, DATAFUSION_PLAN_ARTIFACTS_SPEC, row.to_row())


def pipeline_events_schema() -> pa.Schema:
    """Return the Arrow schema used for the pipeline events Delta table.

    Returns:
    -------
    pyarrow.Schema
        Normalized pipeline events schema.
    """
    from datafusion_engine.schema import DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA

    schema = DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA
    if isinstance(schema, pa.Schema):
        return schema
    return pa.schema(schema)


def bootstrap_pipeline_events_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    table_path: Path,
) -> None:
    """Create or overwrite the pipeline events table with an empty bootstrap commit."""
    schema = pipeline_events_schema()
    empty_table = pa.Table.from_pylist([], schema=schema)
    commit_metadata = {
        "codeanatomy_operation": "pipeline_events_bootstrap",
        "codeanatomy_mode": "overwrite",
        "codeanatomy_table": PIPELINE_EVENTS_TABLE_NAME,
    }
    _write_artifact_table(
        ctx,
        profile,
        request=_ArtifactTableWriteRequest(
            table_path=table_path,
            arrow_table=empty_table,
            commit_metadata=commit_metadata,
            mode="overwrite",
            schema_mode="overwrite",
            operation_id="pipeline_events_bootstrap",
        ),
    )


def refresh_pipeline_events_registration(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    location: DatasetLocation,
) -> None:
    """Re-register the pipeline events table in the active DataFusion context."""
    from datafusion_engine.dataset.registration_core import DataFusionCachePolicy
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(PIPELINE_EVENTS_TABLE_NAME):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(PIPELINE_EVENTS_TABLE_NAME)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    facade.register_dataset(
        name=PIPELINE_EVENTS_TABLE_NAME,
        location=location,
        cache_policy=DataFusionCachePolicy(enabled=False, max_columns=None),
    )


def record_pipeline_events_summary(
    profile: DataFusionRuntimeProfile,
    *,
    rows: Sequence[PipelineEventRow],
    path: str,
    version: int,
) -> None:
    """Emit summary and per-row telemetry artifacts for stored pipeline events."""
    payload = {
        "table": PIPELINE_EVENTS_TABLE_NAME,
        "path": path,
        "row_count": len(rows),
        "event_names": sorted({row.event_name for row in rows}),
        "run_ids": sorted({row.run_id for row in rows}),
        "delta_version": version,
    }
    from serde_artifact_specs import DATAFUSION_PIPELINE_EVENTS_SPEC, PIPELINE_EVENTS_STORE_SPEC

    record_artifact(profile, PIPELINE_EVENTS_STORE_SPEC, payload)
    for row in rows:
        record_artifact(profile, DATAFUSION_PIPELINE_EVENTS_SPEC, row.to_row())


__all__ = [
    "bootstrap_pipeline_events_table",
    "bootstrap_plan_artifacts_table",
    "delta_schema_available",
    "pipeline_events_location",
    "pipeline_events_schema",
    "plan_artifacts_location",
    "plan_artifacts_root",
    "plan_artifacts_schema",
    "profile_name",
    "record_pipeline_events_summary",
    "record_plan_artifact_summary",
    "refresh_pipeline_events_registration",
    "refresh_plan_artifacts_registration",
    "reset_artifacts_table_path",
    "with_delta_settings",
]
