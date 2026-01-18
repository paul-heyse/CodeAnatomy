"""Dataset fingerprint tracking for incremental runs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa

from arrowdsl.schema.build import table_from_arrays
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore

_FINGERPRINTS_FILENAME = "dataset_fingerprints.json"


def _fingerprints_path(state_store: StateStore) -> Path:
    """Return the dataset fingerprint metadata path.

    Returns
    -------
    Path
        Path to the fingerprint metadata file.
    """
    return state_store.metadata_dir() / _FINGERPRINTS_FILENAME


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
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, Mapping):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def write_dataset_fingerprints(state_store: StateStore, fingerprints: Mapping[str, str]) -> None:
    """Persist dataset fingerprints to the state store."""
    state_store.ensure_dirs()
    path = _fingerprints_path(state_store)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {str(key): str(value) for key, value in fingerprints.items()}
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, sort_keys=True, indent=2)


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
