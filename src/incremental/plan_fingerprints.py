"""Plan fingerprint persistence for incremental scheduling."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.schema.build import table_from_arrays
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from ibis_engine.sources import IbisDeltaReadOptions, read_delta_ibis
from incremental.delta_context import DeltaAccessContext
from incremental.ibis_exec import ibis_expr_to_table
from incremental.state_store import StateStore
from storage.deltalake import delta_table_version, enable_delta_features

if TYPE_CHECKING:
    from ibis_engine.execution import IbisExecutionContext
    from storage.deltalake import StorageOptions

PLAN_FINGERPRINTS_VERSION = 1
_PLAN_FINGERPRINTS_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("task_name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
    ]
)
_PLAN_FINGERPRINTS_DIRNAME = "plan_fingerprints"


def _plan_fingerprints_path(state_store: StateStore) -> Path:
    """Return the plan fingerprint metadata path.

    Returns
    -------
    Path
        Path to the plan fingerprint metadata directory.
    """
    return state_store.metadata_dir() / _PLAN_FINGERPRINTS_DIRNAME


def read_plan_fingerprints(
    state_store: StateStore,
    *,
    context: DeltaAccessContext,
) -> dict[str, str]:
    """Read plan fingerprints from the state store.

    Returns
    -------
    dict[str, str]
        Mapping of task names to plan fingerprints.
    """
    path = _plan_fingerprints_path(state_store)
    if not path.exists():
        return {}
    storage = context.storage
    version = delta_table_version(
        str(path),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
    if version is None:
        return {}
    table = _read_delta_table(context, path, name="plan_fingerprints_read")
    results: dict[str, str] = {}
    for row in table.to_pylist():
        if not isinstance(row, Mapping):
            continue
        name = row.get("task_name")
        fingerprint = row.get("plan_fingerprint")
        if name is None or fingerprint is None:
            continue
        results[str(name)] = str(fingerprint)
    return results


def write_plan_fingerprints(
    state_store: StateStore,
    fingerprints: Mapping[str, str],
    *,
    execution: IbisExecutionContext,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> str:
    """Persist plan fingerprints to the state store.

    Returns
    -------
    str
        Delta table path where fingerprints were written.
    """
    state_store.ensure_dirs()
    path = _plan_fingerprints_path(state_store)
    path.parent.mkdir(parents=True, exist_ok=True)
    names = sorted(fingerprints)
    if not names:
        table = table_from_arrays(_PLAN_FINGERPRINTS_SCHEMA, columns={}, num_rows=0)
    else:
        versions = [PLAN_FINGERPRINTS_VERSION] * len(names)
        table = table_from_arrays(
            _PLAN_FINGERPRINTS_SCHEMA,
            columns={
                "version": pa.array(versions, type=pa.int32()),
                "task_name": pa.array(names, type=pa.string()),
                "plan_fingerprint": pa.array(
                    [fingerprints[name] for name in names], type=pa.string()
                ),
            },
            num_rows=len(names),
        )
    result = write_ibis_dataset_delta(
        table,
        str(path),
        options=IbisDatasetWriteOptions(
            execution=execution,
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata={"snapshot_kind": "plan_fingerprints"},
                storage_options=storage_options,
                log_storage_options=log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return result.path


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
    *,
    name: str,
) -> pa.Table:
    backend = context.runtime.ibis_backend()
    table = read_delta_ibis(
        backend,
        str(path),
        options=IbisDeltaReadOptions(storage_options=context.storage.storage_options),
    )
    return ibis_expr_to_table(table, runtime=context.runtime, name=name)


__all__ = ["read_plan_fingerprints", "write_plan_fingerprints"]
