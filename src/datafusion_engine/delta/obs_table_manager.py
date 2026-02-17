"""Delta observability table lifecycle helpers."""

from __future__ import annotations

import contextlib
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.dataset as ds

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.obs_schemas import (
    delta_maintenance_schema,
    delta_mutation_schema,
    delta_scan_plan_schema,
    delta_snapshot_schema,
)
from datafusion_engine.errors import DataFusionEngineError
from datafusion_engine.io.adapter import DataFusionIOAdapter
from datafusion_engine.session.facade import DataFusionExecutionFacade

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

DELTA_SNAPSHOT_TABLE_NAME = "datafusion_delta_snapshots_v2"
DELTA_MUTATION_TABLE_NAME = "datafusion_delta_mutations_v2"
DELTA_SCAN_PLAN_TABLE_NAME = "datafusion_delta_scan_plans_v2"
DELTA_MAINTENANCE_TABLE_NAME = "datafusion_delta_maintenance_v2"

try:
    _DEFAULT_OBSERVABILITY_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_OBSERVABILITY_ROOT = Path.cwd() / ".artifacts"


def observability_root(profile: DataFusionRuntimeProfile) -> Path:
    """Return root directory for Delta observability tables."""
    root_value = profile.policies.plan_artifacts_root
    root = Path(root_value) if root_value else _DEFAULT_OBSERVABILITY_ROOT
    return root / "delta_observability"


def observability_commit_metadata(
    operation: str,
    metadata: dict[str, str] | None,
) -> dict[str, str]:
    """Return sanitized commit metadata for observability writes."""
    sanitized: dict[str, str] = {
        str(key): str(value)
        for key, value in (metadata or {}).items()
        if str(key).lower() != "operation"
    }
    sanitized["observability_operation"] = operation
    return sanitized


def is_observability_target(
    *,
    dataset_name: str | None,
    table_uri: str | None,
) -> bool:
    """Return whether target points at an observability table."""
    names = {
        DELTA_SNAPSHOT_TABLE_NAME,
        DELTA_MUTATION_TABLE_NAME,
        DELTA_SCAN_PLAN_TABLE_NAME,
        DELTA_MAINTENANCE_TABLE_NAME,
    }
    if dataset_name in names:
        return True
    if table_uri is None:
        return False
    return Path(table_uri).name in names


def ensure_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    schema: pa.Schema,
) -> DatasetLocation | None:
    """Ensure a Delta observability table exists and is registered.

    Returns:
        DatasetLocation | None: Registered observability table location or
            ``None`` when bootstrap/registration fails.
    """
    table_path = observability_root(profile) / name
    delta_log_path = table_path / "_delta_log"
    has_delta_log = delta_log_path.exists() and any(delta_log_path.glob("*.json"))
    from serde_artifact_specs import (
        DELTA_OBSERVABILITY_BOOTSTRAP_COMPLETED_SPEC,
        DELTA_OBSERVABILITY_BOOTSTRAP_FAILED_SPEC,
        DELTA_OBSERVABILITY_BOOTSTRAP_STARTED_SPEC,
        DELTA_OBSERVABILITY_REGISTER_FAILED_SPEC,
    )

    if not has_delta_log:
        profile.record_artifact(
            DELTA_OBSERVABILITY_BOOTSTRAP_STARTED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "operation": "delta_observability_bootstrap",
            },
        )
        try:
            bootstrap_observability_table(
                ctx,
                profile,
                table_path=table_path,
                schema=schema,
                operation="delta_observability_bootstrap",
            )
        except (
            RuntimeError,
            TypeError,
            ValueError,
            OSError,
            ImportError,
        ) as exc:
            profile.record_artifact(
                DELTA_OBSERVABILITY_BOOTSTRAP_FAILED_SPEC,
                {
                    "event_time_unix_ms": int(time.time() * 1000),
                    "table": name,
                    "path": str(table_path),
                    "operation": "delta_observability_bootstrap",
                    "error": str(exc),
                },
            )
            return None
        profile.record_artifact(
            DELTA_OBSERVABILITY_BOOTSTRAP_COMPLETED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "operation": "delta_observability_bootstrap",
            },
        )
    if not ensure_observability_schema(
        ctx,
        profile,
        table_path=table_path,
        schema=schema,
        operation="delta_observability_schema_reset",
    ):
        return None
    location = DatasetLocation(path=str(table_path), format="delta")
    try:
        DataFusionExecutionFacade(
            ctx=ctx,
            runtime_profile=profile,
        ).register_dataset(
            name=name,
            location=location,
            overwrite=True,
        )
    except (
        DataFusionEngineError,
        RuntimeError,
        ValueError,
        TypeError,
        OSError,
        KeyError,
    ) as exc:
        if register_observability_fallback(
            ctx,
            profile,
            name=name,
            table_path=table_path,
            exc=exc,
        ):
            return location
        profile.record_artifact(
            DELTA_OBSERVABILITY_REGISTER_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(exc),
            },
        )
        return None
    return location


def ensure_delta_observability_tables(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
) -> dict[str, DatasetLocation | None]:
    """Ensure all Delta observability tables exist and are registered.

    Returns:
        dict[str, DatasetLocation | None]: Mapping of observability table names
            to registration locations.
    """
    specs = (
        (DELTA_SNAPSHOT_TABLE_NAME, delta_snapshot_schema()),
        (DELTA_MUTATION_TABLE_NAME, delta_mutation_schema()),
        (DELTA_SCAN_PLAN_TABLE_NAME, delta_scan_plan_schema()),
        (DELTA_MAINTENANCE_TABLE_NAME, delta_maintenance_schema()),
    )
    return {
        name: ensure_observability_table(
            ctx,
            profile,
            name=name,
            schema=schema,
        )
        for name, schema in specs
    }


def register_observability_fallback(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    table_path: Path,
    exc: Exception,
) -> bool:
    """Fallback dataset registration when control-plane registration fails.

    Returns:
        bool: ``True`` when fallback registration succeeds.
    """
    from serde_artifact_specs import (
        DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC,
        DELTA_OBSERVABILITY_REGISTER_FALLBACK_USED_SPEC,
    )

    if not is_control_plane_registration_error(exc):
        return False
    try:
        dataset = ds.dataset(str(table_path), format="parquet")
    except (RuntimeError, TypeError, ValueError, OSError) as dataset_exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(dataset_exc),
                "cause": str(exc),
                "stage": "dataset",
            },
        )
        return False
    adapter = DataFusionIOAdapter(ctx=ctx, profile=profile)
    if ctx.table_exist(name):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            adapter.deregister_table(name)
    try:
        adapter.register_dataset(name, dataset)
    except (RuntimeError, TypeError, ValueError, OSError, KeyError) as register_exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_REGISTER_FALLBACK_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": name,
                "path": str(table_path),
                "error": str(register_exc),
                "cause": str(exc),
                "stage": "register",
            },
        )
        return False
    profile.record_artifact(
        DELTA_OBSERVABILITY_REGISTER_FALLBACK_USED_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": name,
            "path": str(table_path),
            "cause": str(exc),
        },
    )
    return True


def is_control_plane_registration_error(exc: Exception) -> bool:
    """Return whether error indicates control-plane registration degradation."""
    if not isinstance(exc, DataFusionEngineError):
        return False
    message = str(exc).lower()
    return "control-plane failed" in message or "degraded python fallback paths" in message


def ensure_observability_schema(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> bool:
    """Ensure the on-disk table schema matches expected observability schema.

    Returns:
        bool: ``True`` when schema matches (or is repaired) successfully.
    """
    from serde_artifact_specs import (
        DELTA_OBSERVABILITY_SCHEMA_CHECK_FAILED_SPEC,
        DELTA_OBSERVABILITY_SCHEMA_DRIFT_SPEC,
        DELTA_OBSERVABILITY_SCHEMA_RESET_FAILED_SPEC,
    )

    table_name = table_path.name
    try:
        from arro3.core import Schema as Arro3Schema
        from deltalake import DeltaTable

        current_schema = DeltaTable(str(table_path)).schema().to_arrow()
        expected_schema = Arro3Schema.from_arrow(schema)
    except (
        RuntimeError,
        TypeError,
        ValueError,
        OSError,
        ImportError,
        AttributeError,
    ) as exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_SCHEMA_CHECK_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": table_name,
                "path": str(table_path),
                "operation": operation,
                "error": str(exc),
            },
        )
        return False
    if current_schema.equals(expected_schema):
        return True
    profile.record_artifact(
        DELTA_OBSERVABILITY_SCHEMA_DRIFT_SPEC,
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "table": table_name,
            "path": str(table_path),
            "operation": operation,
            "expected_fields": list(expected_schema.names),
            "observed_fields": list(current_schema.names),
        },
    )
    try:
        bootstrap_observability_table(
            ctx,
            profile,
            table_path=table_path,
            schema=schema,
            operation=operation,
        )
    except (
        RuntimeError,
        TypeError,
        ValueError,
        OSError,
        ImportError,
    ) as exc:
        profile.record_artifact(
            DELTA_OBSERVABILITY_SCHEMA_RESET_FAILED_SPEC,
            {
                "event_time_unix_ms": int(time.time() * 1000),
                "table": table_name,
                "path": str(table_path),
                "operation": operation,
                "error": str(exc),
            },
        )
        return False
    return True


def bootstrap_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> None:
    """Create or reset an observability table at the target path.

    Raises:
        CommitFailedError: If Delta commit fails for non-corrupt-log reasons.
        DeltaError: If Delta operations fail for non-corrupt-log reasons.
        DeltaProtocolError: If Delta protocol operations fail.
        OSError: If filesystem operations fail during bootstrap.
        RuntimeError: If runtime write execution fails.
        SchemaMismatchError: If the write request schema is invalid.
        TableNotFoundError: If target table metadata cannot be resolved.
        TypeError: If write request payload is invalid.
        ValueError: If write options are invalid.
    """
    _ = profile
    table_path.parent.mkdir(parents=True, exist_ok=True)
    bootstrap_row = bootstrap_observability_row(schema)
    empty = pa.Table.from_pylist([bootstrap_row], schema=schema)
    from deltalake.exceptions import (
        CommitFailedError,
        DeltaError,
        DeltaProtocolError,
        SchemaMismatchError,
        TableNotFoundError,
    )

    from datafusion_engine.delta.transactions import write_transaction
    from datafusion_engine.delta.write_ipc_payload import (
        DeltaWriteRequestOptions,
        build_delta_write_request,
    )
    from storage.deltalake.delta_runtime_ops import delta_commit_options

    commit_options = delta_commit_options(
        commit_properties=None,
        commit_metadata=observability_commit_metadata(
            operation,
            {"table": table_path.name},
        ),
        app_id=None,
        app_version=None,
    )
    request = build_delta_write_request(
        table_uri=str(table_path),
        table=empty,
        options=DeltaWriteRequestOptions(
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=None,
            commit_options=commit_options,
        ),
    )
    try:
        write_transaction(ctx, request=request)
    except (
        CommitFailedError,
        DeltaError,
        DeltaProtocolError,
        OSError,
        RuntimeError,
        SchemaMismatchError,
        TableNotFoundError,
        TypeError,
        ValueError,
    ) as exc:
        if not is_corrupt_delta_log_error(exc):
            raise
        with contextlib.suppress(OSError):
            shutil.rmtree(table_path)
        table_path.parent.mkdir(parents=True, exist_ok=True)
        write_transaction(ctx, request=request)


def bootstrap_observability_row(schema: pa.Schema) -> dict[str, object]:
    """Build a one-row bootstrap payload compatible with schema nullability.

    Returns:
        dict[str, object]: Bootstrap row matching the provided schema.
    """
    row: dict[str, object] = {}
    for field in schema:
        if field.nullable:
            row[field.name] = None
            continue
        dtype = field.type
        if pa.types.is_string(dtype):
            row[field.name] = "__bootstrap__"
        elif pa.types.is_integer(dtype):
            row[field.name] = 0
        elif pa.types.is_binary(dtype):
            row[field.name] = b""
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            row[field.name] = []
        elif pa.types.is_map(dtype):
            row[field.name] = {}
        else:
            row[field.name] = None
    return row


def is_schema_mismatch_error(exc: Exception) -> bool:
    """Return whether error message indicates schema mismatch."""
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "schemamismatcherror",
            "schema mismatch",
            "cannot cast schema",
            "number of fields does not match",
        )
    )


def is_corrupt_delta_log_error(exc: Exception) -> bool:
    """Return whether error indicates corrupt Delta transaction log."""
    message = str(exc).lower()
    return "invalid json in log record" in message or "duplicate field `operation`" in message


__all__ = [
    "DELTA_MAINTENANCE_TABLE_NAME",
    "DELTA_MUTATION_TABLE_NAME",
    "DELTA_SCAN_PLAN_TABLE_NAME",
    "DELTA_SNAPSHOT_TABLE_NAME",
    "bootstrap_observability_row",
    "bootstrap_observability_table",
    "ensure_delta_observability_tables",
    "ensure_observability_schema",
    "ensure_observability_table",
    "is_control_plane_registration_error",
    "is_corrupt_delta_log_error",
    "is_observability_target",
    "is_schema_mismatch_error",
    "observability_commit_metadata",
    "observability_root",
    "register_observability_fallback",
]
