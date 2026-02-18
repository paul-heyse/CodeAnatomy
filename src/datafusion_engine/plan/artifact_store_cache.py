"""Cache-focused helpers for the plan artifact store."""

from __future__ import annotations

from pathlib import Path

from datafusion import SessionContext

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.plan.artifact_store_core import (
    PIPELINE_EVENTS_TABLE_NAME,
    PLAN_ARTIFACTS_TABLE_NAME,
)
from datafusion_engine.plan.artifact_store_tables import (
    bootstrap_pipeline_events_table,
    bootstrap_plan_artifacts_table,
    delta_schema_available,
    pipeline_events_location,
    plan_artifacts_location,
    refresh_pipeline_events_registration,
    refresh_plan_artifacts_registration,
    reset_artifacts_table_path,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def ensure_plan_artifacts_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure plan-artifact cache table exists.

    Returns:
    -------
    DatasetLocation | None
        Resolved artifacts table location when configured.
    """
    location = plan_artifacts_location(profile)
    if location is None:
        return None
    table_path = Path(location.path)
    existing_version = profile.delta_ops.delta_service().table_version(path=str(table_path))
    if existing_version is None:
        if table_path.exists():
            reset_artifacts_table_path(
                profile,
                table_path,
                table_name=PLAN_ARTIFACTS_TABLE_NAME,
                reason="delta_table_version_unavailable",
            )
        bootstrap_plan_artifacts_table(ctx, profile, table_path)
    elif not delta_schema_available(location, profile=profile):
        reset_artifacts_table_path(
            profile,
            table_path,
            table_name=PLAN_ARTIFACTS_TABLE_NAME,
            reason="delta_schema_unavailable",
        )
        bootstrap_plan_artifacts_table(ctx, profile, table_path)
    refresh_plan_artifacts_registration(ctx, profile, location)
    return location


def ensure_pipeline_events_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> DatasetLocation | None:
    """Ensure pipeline-event cache table exists.

    Returns:
    -------
    DatasetLocation | None
        Resolved pipeline-events table location when configured.
    """
    location = pipeline_events_location(profile)
    if location is None:
        return None
    table_path = Path(location.path)
    existing_version = profile.delta_ops.delta_service().table_version(path=str(table_path))
    if existing_version is None:
        if table_path.exists():
            reset_artifacts_table_path(
                profile,
                table_path,
                table_name=PIPELINE_EVENTS_TABLE_NAME,
                reason="delta_table_version_unavailable",
            )
        bootstrap_pipeline_events_table(ctx, profile, table_path)
    elif not delta_schema_available(location, profile=profile):
        reset_artifacts_table_path(
            profile,
            table_path,
            table_name=PIPELINE_EVENTS_TABLE_NAME,
            reason="delta_schema_unavailable",
        )
        bootstrap_pipeline_events_table(ctx, profile, table_path)
    refresh_pipeline_events_registration(ctx, profile, location)
    return location


__all__ = [
    "ensure_pipeline_events_table",
    "ensure_plan_artifacts_table",
]
