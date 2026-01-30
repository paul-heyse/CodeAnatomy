"""Dataset fingerprint tracking for incremental runs."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.build import empty_table, table_from_columns
from datafusion_engine.arrow.schema import (
    dataset_name_field,
    fingerprint_field,
    version_field,
)
from datafusion_engine.io.write import WriteMode
from incremental.delta_context import read_delta_table_via_facade
from incremental.registry_specs import dataset_schema
from incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)
from storage.deltalake import delta_table_version

if TYPE_CHECKING:
    from incremental.delta_context import DeltaAccessContext
    from incremental.state_store import StateStore

FINGERPRINTS_VERSION = 1
_FINGERPRINTS_SCHEMA = pa.schema(
    [
        version_field(),
        dataset_name_field(),
        fingerprint_field(),
    ]
)
_FINGERPRINTS_DIRNAME = "dataset_fingerprints"


def _fingerprints_path(state_store: StateStore) -> Path:
    """Return the dataset fingerprint metadata path.

    Returns
    -------
    Path
        Path to the fingerprint metadata file.
    """
    return state_store.metadata_dir() / _FINGERPRINTS_DIRNAME


def read_dataset_fingerprints(
    state_store: StateStore,
    *,
    context: DeltaAccessContext,
) -> dict[str, str]:
    """Read the dataset fingerprint mapping from the state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to fingerprint hashes.
    """
    path = _fingerprints_path(state_store)
    if not path.exists():
        return {}
    resolved = context.resolve_storage(table_uri=str(path))
    version = delta_table_version(
        str(path),
        storage_options=resolved.storage_options,
        log_storage_options=resolved.log_storage_options,
    )
    if version is None:
        return {}
    table = _read_delta_table(context, path, name="dataset_fingerprints_read")
    results: dict[str, str] = {}
    for row in table.to_pylist():
        if not isinstance(row, Mapping):
            continue
        name = row.get("dataset_name")
        fingerprint = row.get("fingerprint")
        if name is None or fingerprint is None:
            continue
        results[str(name)] = str(fingerprint)
    return results


def write_dataset_fingerprints(
    state_store: StateStore,
    fingerprints: Mapping[str, str],
    *,
    context: DeltaAccessContext,
) -> None:
    """Persist dataset fingerprints to the state store."""
    state_store.ensure_dirs()
    path = _fingerprints_path(state_store)
    path.parent.mkdir(parents=True, exist_ok=True)
    names = sorted(fingerprints)
    if not names:
        table = empty_table(_FINGERPRINTS_SCHEMA)
    else:
        versions = [FINGERPRINTS_VERSION] * len(names)
        table = table_from_columns(
            _FINGERPRINTS_SCHEMA,
            {
                "version": pa.array(versions, type=pa.int32()),
                "dataset_name": pa.array(names, type=pa.string()),
                "fingerprint": pa.array([fingerprints[name] for name in names], type=pa.string()),
            },
        )
    resolved_storage = context.resolve_storage(table_uri=str(path))
    write_delta_table_via_pipeline(
        runtime=context.runtime,
        table=table,
        request=IncrementalDeltaWriteRequest(
            destination=str(path),
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata={"snapshot_kind": "dataset_fingerprints"},
            storage_options=resolved_storage.storage_options,
            log_storage_options=resolved_storage.log_storage_options,
            operation_id="incremental_dataset_fingerprints",
        ),
    )


def output_fingerprint_change_table(
    previous: Mapping[str, str],
    current: Mapping[str, str],
) -> pa.Table:
    """Return a table describing output fingerprint changes.

    Returns
    -------
    pa.Table
        Table aligned to ``inc_output_fingerprint_changes_v1``.
    """
    schema = dataset_schema("inc_output_fingerprint_changes_v1")
    names = sorted(set(previous) | set(current))
    if not names:
        return empty_table(schema)
    change_kind: list[str] = []
    prev_fps: list[str | None] = []
    cur_fps: list[str | None] = []
    for name in names:
        prev = previous.get(name)
        cur = current.get(name)
        prev_fps.append(prev)
        cur_fps.append(cur)
        if prev is None and cur is not None:
            change_kind.append("added")
        elif prev is not None and cur is None:
            change_kind.append("removed")
        elif prev != cur:
            change_kind.append("changed")
        else:
            change_kind.append("unchanged")
    return table_from_columns(
        schema,
        {
            "dataset_name": pa.array(names, type=pa.string()),
            "change_kind": pa.array(change_kind, type=pa.string()),
            "prev_fingerprint": pa.array(prev_fps, type=pa.string()),
            "cur_fingerprint": pa.array(cur_fps, type=pa.string()),
        },
    )


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
    *,
    name: str,
) -> pa.Table:
    return read_delta_table_via_facade(context, path=path, name=name)


__all__ = [
    "output_fingerprint_change_table",
    "read_dataset_fingerprints",
    "write_dataset_fingerprints",
]
