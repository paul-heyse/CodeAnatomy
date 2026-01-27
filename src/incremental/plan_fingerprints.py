"""Plan fingerprint persistence for incremental scheduling.

Plan fingerprints are based on DataFusion plan bundles and Substrait bytes,
replacing SQLGlot AST-based fingerprints.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from arrowdsl.schema.build import table_from_arrays
from incremental.delta_context import read_delta_table_via_facade
from storage.deltalake import delta_table_version, enable_delta_features

if TYPE_CHECKING:
    from incremental.delta_context import DeltaAccessContext
    from incremental.state_store import StateStore
    from storage.deltalake import StorageOptions

PLAN_FINGERPRINTS_VERSION = 5  # Incremented for runtime-aware task signatures
_PLAN_FINGERPRINTS_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("task_name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
        pa.field("plan_task_signature", pa.string(), nullable=False),
    ]
)
_PLAN_FINGERPRINTS_DIRNAME = "plan_fingerprints"


@dataclass(frozen=True)
class PlanFingerprintSnapshot:
    """Plan fingerprint snapshot.

    Plan snapshots capture both the DataFusion plan fingerprint and a
    runtime-aware task signature that includes session identity.

    Attributes
    ----------
    plan_fingerprint : str
        SHA256 hash of Substrait bytes or optimized plan display.
    plan_task_signature : str
        Runtime-aware task signature used for incremental diffs.
    substrait_bytes : bytes | None
        Optional Substrait serialization for portable plan storage.
    """

    plan_fingerprint: str
    plan_task_signature: str = ""
    substrait_bytes: bytes | None = None


def _plan_fingerprints_path(state_store: StateStore) -> Path:
    """Return the plan fingerprint metadata path.

    Returns
    -------
    Path
        Path to the plan fingerprint metadata directory.
    """
    return state_store.metadata_dir() / _PLAN_FINGERPRINTS_DIRNAME


def read_plan_snapshots(
    state_store: StateStore,
    *,
    context: DeltaAccessContext,
) -> dict[str, PlanFingerprintSnapshot]:
    """Read plan fingerprints from the state store.

    Returns
    -------
    dict[str, PlanFingerprintSnapshot]
        Mapping of task names to plan snapshot metadata.
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
    results: dict[str, PlanFingerprintSnapshot] = {}
    for row in table.to_pylist():
        if not isinstance(row, Mapping):
            continue
        name = row.get("task_name")
        fingerprint = row.get("plan_fingerprint")
        task_signature = row.get("plan_task_signature")
        if name is None or fingerprint is None:
            continue
        signature_value = str(task_signature) if task_signature else str(fingerprint)
        results[str(name)] = PlanFingerprintSnapshot(
            plan_fingerprint=str(fingerprint),
            plan_task_signature=signature_value,
        )
    return results


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
    snapshots = read_plan_snapshots(state_store, context=context)
    return {name: snap.plan_fingerprint for name, snap in snapshots.items()}


def write_plan_snapshots(
    state_store: StateStore,
    snapshots: Mapping[str, PlanFingerprintSnapshot],
    *,
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
    names = sorted(snapshots)
    if not names:
        table = table_from_arrays(_PLAN_FINGERPRINTS_SCHEMA, columns={}, num_rows=0)
    else:
        versions = [PLAN_FINGERPRINTS_VERSION] * len(names)
        fingerprints = [snapshots[name].plan_fingerprint for name in names]
        task_signatures = [
            snapshots[name].plan_task_signature or snapshots[name].plan_fingerprint
            for name in names
        ]
        table = table_from_arrays(
            _PLAN_FINGERPRINTS_SCHEMA,
            columns={
                "version": pa.array(versions, type=pa.int32()),
                "task_name": pa.array(names, type=pa.string()),
                "plan_fingerprint": pa.array(fingerprints, type=pa.string()),
                "plan_task_signature": pa.array(task_signatures, type=pa.string()),
            },
            num_rows=len(names),
        )
    from deltalake import CommitProperties, write_deltalake

    resolved_storage = _resolve_storage_options(
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    write_deltalake(
        str(path),
        cast("pa.Table", table),
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=resolved_storage,
        commit_properties=CommitProperties(
            custom_metadata={"snapshot_kind": "plan_fingerprints"},
        ),
    )
    enable_delta_features(
        str(path),
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )
    return str(path)


def write_plan_fingerprints(
    state_store: StateStore,
    fingerprints: Mapping[str, str],
    *,
    storage_options: StorageOptions | None = None,
    log_storage_options: StorageOptions | None = None,
) -> str:
    """Persist plan fingerprints to the state store.

    Returns
    -------
    str
        Delta table path where fingerprints were written.
    """
    snapshots = {
        name: PlanFingerprintSnapshot(
            plan_fingerprint=fingerprint,
            plan_task_signature=fingerprint,
        )
        for name, fingerprint in fingerprints.items()
    }
    return write_plan_snapshots(
        state_store,
        snapshots,
        storage_options=storage_options,
        log_storage_options=log_storage_options,
    )


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
    *,
    name: str,
) -> pa.Table:
    return read_delta_table_via_facade(context, path=path, name=name)


def _resolve_storage_options(
    *,
    storage_options: StorageOptions | None,
    log_storage_options: StorageOptions | None,
) -> dict[str, str] | None:
    if storage_options is None and log_storage_options is None:
        return None
    resolved = dict(storage_options or {})
    if log_storage_options is not None:
        resolved.update(dict(log_storage_options))
    return resolved or None


__all__ = [
    "PlanFingerprintSnapshot",
    "read_plan_fingerprints",
    "read_plan_snapshots",
    "write_plan_fingerprints",
    "write_plan_snapshots",
]
