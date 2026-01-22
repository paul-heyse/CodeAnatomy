"""Dataset fingerprint tracking for incremental runs."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa

from arrowdsl.schema.build import table_from_arrays
from datafusion_engine.runtime import read_delta_as_reader
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore
from storage.deltalake import (
    DeltaWriteOptions,
    enable_delta_features,
    write_table_delta,
)

FINGERPRINTS_VERSION = 1
_FINGERPRINTS_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("dataset_name", pa.string(), nullable=False),
        pa.field("fingerprint", pa.string(), nullable=False),
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


def read_dataset_fingerprints(state_store: StateStore) -> dict[str, str]:
    """Read the dataset fingerprint mapping from the state store.

    Returns
    -------
    dict[str, str]
        Mapping of dataset names to fingerprint hashes.
    """
    path = _fingerprints_path(state_store)
    if not path.exists():
        return {}
    reader = read_delta_as_reader(str(path))
    table = reader.read_all()
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


def write_dataset_fingerprints(state_store: StateStore, fingerprints: Mapping[str, str]) -> None:
    """Persist dataset fingerprints to the state store."""
    state_store.ensure_dirs()
    path = _fingerprints_path(state_store)
    path.parent.mkdir(parents=True, exist_ok=True)
    names = sorted(fingerprints)
    if not names:
        table = table_from_arrays(_FINGERPRINTS_SCHEMA, columns={}, num_rows=0)
    else:
        versions = [FINGERPRINTS_VERSION] * len(names)
        table = table_from_arrays(
            _FINGERPRINTS_SCHEMA,
            columns={
                "version": pa.array(versions, type=pa.int32()),
                "dataset_name": pa.array(names, type=pa.string()),
                "fingerprint": pa.array([fingerprints[name] for name in names], type=pa.string()),
            },
            num_rows=len(names),
        )
    result = write_table_delta(
        table,
        str(path),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={"snapshot_kind": "dataset_fingerprints"},
        ),
    )
    enable_delta_features(result.path)


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
        return table_from_arrays(schema, columns={}, num_rows=0)
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
    return table_from_arrays(
        schema,
        columns={
            "dataset_name": pa.array(names, type=pa.string()),
            "change_kind": pa.array(change_kind, type=pa.string()),
            "prev_fingerprint": pa.array(prev_fps, type=pa.string()),
            "cur_fingerprint": pa.array(cur_fps, type=pa.string()),
        },
        num_rows=len(names),
    )


__all__ = [
    "output_fingerprint_change_table",
    "read_dataset_fingerprints",
    "write_dataset_fingerprints",
]
