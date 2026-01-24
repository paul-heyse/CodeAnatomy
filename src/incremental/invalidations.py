"""Invalidation helpers for incremental state snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from arrowdsl.core.ordering import Ordering
from engine.runtime_profile import runtime_profile_snapshot
from ibis_engine.execution import materialize_ibis_plan
from ibis_engine.io_bridge import (
    IbisDatasetWriteOptions,
    IbisDeltaWriteOptions,
    write_ibis_dataset_delta,
)
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import IbisDeltaReadOptions, read_delta_ibis
from incremental.delta_context import DeltaAccessContext
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from sqlglot_tools.optimizer import sqlglot_policy_snapshot_for
from storage.deltalake import delta_table_version, enable_delta_features
from storage.ipc import payload_hash

INVALIDATION_SNAPSHOT_VERSION = 1
_PLAN_HASH_ENTRY = pa.struct(
    [
        pa.field("plan_name", pa.string(), nullable=False),
        pa.field("plan_hash", pa.string(), nullable=False),
    ]
)
_INVALIDATION_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("incremental_plan_hashes", pa.list_(_PLAN_HASH_ENTRY), nullable=False),
        pa.field("incremental_metadata_hash", pa.string(), nullable=True),
        pa.field("runtime_profile_hash", pa.string(), nullable=True),
    ]
)
_INCREMENTAL_METADATA_SCHEMA = pa.schema(
    [
        pa.field("datafusion_settings_hash", pa.string(), nullable=False),
        pa.field("runtime_profile_hash", pa.string(), nullable=False),
        pa.field("sqlglot_policy_hash", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class InvalidationSnapshot:
    """Persisted signatures for incremental invalidation checks."""

    incremental_plan_hashes: dict[str, str]
    incremental_metadata_hash: str | None
    runtime_profile_hash: str | None

    def to_payload(self) -> dict[str, object]:
        """Return an Arrow-friendly payload for snapshot persistence.

        Returns
        -------
        dict[str, object]
            Payload suitable for IPC serialization.
        """
        return {
            "version": INVALIDATION_SNAPSHOT_VERSION,
            "incremental_plan_hashes": _plan_hash_entries(self.incremental_plan_hashes),
            "incremental_metadata_hash": self.incremental_metadata_hash,
            "runtime_profile_hash": self.runtime_profile_hash,
        }


@dataclass(frozen=True)
class InvalidationResult:
    """Outcome for an invalidation comparison."""

    invalidated: bool
    reasons: tuple[str, ...] = ()


def build_invalidation_snapshot(
    context: DeltaAccessContext,
    *,
    state_store: StateStore | None = None,
) -> InvalidationSnapshot:
    """Build the current invalidation snapshot.

    Returns
    -------
    InvalidationSnapshot
        Snapshot of plan hashes + runtime metadata.
    """
    runtime = context.runtime
    return InvalidationSnapshot(
        incremental_plan_hashes=_incremental_plan_hashes(
            context=context,
            state_store=state_store,
        ),
        incremental_metadata_hash=_incremental_metadata_hash(runtime),
        runtime_profile_hash=_runtime_profile_hash(runtime),
    )


def read_invalidation_snapshot(
    state_store: StateStore,
    *,
    context: DeltaAccessContext,
) -> InvalidationSnapshot | None:
    """Read the prior invalidation snapshot if present.

    Returns
    -------
    InvalidationSnapshot | None
        Snapshot when present, otherwise None.

    Raises
    ------
    TypeError
        Raised when the snapshot payload is invalid.
    """
    path = state_store.invalidation_snapshot_path()
    if not path.exists():
        return None
    storage = context.storage
    version = delta_table_version(
        str(path),
        storage_options=storage.storage_options,
        log_storage_options=storage.log_storage_options,
    )
    if version is None:
        return None
    table = _read_delta_table(context, path)
    rows = table.to_pylist()
    if not rows:
        return None
    row = rows[0]
    if not isinstance(row, Mapping):
        msg = "Invalid invalidation snapshot payload."
        raise TypeError(msg)
    return _snapshot_from_row(row)


def write_invalidation_snapshot(
    state_store: StateStore,
    snapshot: InvalidationSnapshot,
    *,
    context: DeltaAccessContext,
) -> str:
    """Persist the invalidation snapshot to the state store.

    Returns
    -------
    str
        Delta table path for the snapshot.
    """
    state_store.ensure_dirs()
    path = state_store.invalidation_snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = snapshot.to_payload()
    table = pa.Table.from_pylist([payload], schema=_INVALIDATION_SCHEMA)
    result = write_ibis_dataset_delta(
        table,
        str(path),
        options=IbisDatasetWriteOptions(
            execution=context.runtime.ibis_execution(),
            writer_strategy="datafusion",
            delta_options=IbisDeltaWriteOptions(
                mode="overwrite",
                schema_mode="overwrite",
                commit_metadata={"snapshot_kind": "incremental_invalidation_snapshot"},
                storage_options=context.storage.storage_options,
                log_storage_options=context.storage.log_storage_options,
            ),
        ),
    )
    enable_delta_features(
        result.path,
        storage_options=context.storage.storage_options,
        log_storage_options=context.storage.log_storage_options,
    )
    return result.path


def diff_invalidation_snapshots(
    previous: InvalidationSnapshot,
    current: InvalidationSnapshot,
) -> tuple[str, ...]:
    """Return reason codes for invalidation differences.

    Returns
    -------
    tuple[str, ...]
        Reason codes describing invalidation triggers.
    """
    reasons: list[str] = []
    if previous.runtime_profile_hash != current.runtime_profile_hash:
        reasons.append("runtime_profile_hash")
    if previous.incremental_metadata_hash != current.incremental_metadata_hash:
        reasons.append("incremental_metadata")
    reasons.extend(
        _diff_mapping(
            previous.incremental_plan_hashes,
            current.incremental_plan_hashes,
            prefix="incremental_plan_hash",
        )
    )
    return tuple(reasons)


def check_state_store_invalidation(
    *,
    state_store: StateStore,
    context: DeltaAccessContext,
) -> InvalidationResult:
    """Compare the current snapshot to the stored snapshot.

    Returns
    -------
    InvalidationResult
        Result describing whether invalidation is required.
    """
    current = build_invalidation_snapshot(context, state_store=state_store)
    previous = read_invalidation_snapshot(state_store, context=context)
    if previous is None:
        write_invalidation_snapshot(state_store, current, context=context)
        return InvalidationResult(invalidated=False, reasons=("missing_snapshot",))
    reasons = diff_invalidation_snapshots(previous, current)
    if reasons:
        write_invalidation_snapshot(state_store, current, context=context)
        return InvalidationResult(invalidated=True, reasons=reasons)
    return InvalidationResult(invalidated=False, reasons=())


def _incremental_metadata_hash(runtime: IncrementalRuntime) -> str | None:
    runtime_snapshot = runtime_profile_snapshot(runtime.execution_ctx.runtime)
    policy_snapshot = sqlglot_policy_snapshot_for(runtime.sqlglot_policy)
    payload = {
        "datafusion_settings_hash": runtime.profile.settings_hash(),
        "runtime_profile_hash": runtime_snapshot.profile_hash,
        "sqlglot_policy_hash": policy_snapshot.policy_hash,
    }
    return payload_hash(payload, _INCREMENTAL_METADATA_SCHEMA)


def _runtime_profile_hash(runtime: IncrementalRuntime) -> str | None:
    return runtime_profile_snapshot(runtime.execution_ctx.runtime).profile_hash


def _incremental_plan_hashes(
    *,
    context: DeltaAccessContext,
    state_store: StateStore | None,
) -> dict[str, str]:
    runtime = context.runtime
    sink = runtime.profile.diagnostics_sink
    if sink is not None:
        artifacts = sink.artifacts_snapshot().get("incremental_sqlglot_plan_v1")
        if artifacts:
            return _plan_hashes_from_artifacts(artifacts)
    if state_store is None:
        return {}
    path = state_store.sqlglot_artifacts_path()
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
    table = _read_delta_table(context, path)
    rows = table.to_pylist()
    return _plan_hashes_from_artifacts(rows)


def _plan_hashes_from_artifacts(
    artifacts: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for payload in artifacts:
        name = payload.get("name")
        plan_hash = payload.get("plan_hash")
        if not name or not plan_hash:
            continue
        resolved[str(name)] = str(plan_hash)
    return resolved


def _plan_hash_entries(plan_hashes: Mapping[str, str]) -> list[dict[str, object]]:
    return [
        {"plan_name": name, "plan_hash": plan_hash}
        for name, plan_hash in sorted(plan_hashes.items())
    ]


def _diff_mapping(
    previous: Mapping[str, str],
    current: Mapping[str, str],
    *,
    prefix: str,
) -> list[str]:
    reasons: list[str] = []
    previous_keys = set(previous)
    current_keys = set(current)
    for name in sorted(previous_keys | current_keys):
        prev = previous.get(name)
        cur = current.get(name)
        if prev is None or cur is None:
            reasons.append(f"{prefix}_missing:{name}")
        elif prev != cur:
            reasons.append(f"{prefix}_changed:{name}")
    return reasons


def _snapshot_from_row(row: Mapping[str, object]) -> InvalidationSnapshot:
    plan_hashes = _coerce_plan_hash_entries(row.get("incremental_plan_hashes"))
    metadata_hash = row.get("incremental_metadata_hash")
    runtime_hash = row.get("runtime_profile_hash")
    return InvalidationSnapshot(
        incremental_plan_hashes=plan_hashes,
        incremental_metadata_hash=str(metadata_hash) if metadata_hash is not None else None,
        runtime_profile_hash=str(runtime_hash) if runtime_hash is not None else None,
    )


def _coerce_plan_hash_entries(value: object) -> dict[str, str]:
    if not isinstance(value, Sequence):
        return {}
    resolved: dict[str, str] = {}
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        name = entry.get("plan_name")
        plan_hash = entry.get("plan_hash")
        if name is None or plan_hash is None:
            continue
        resolved[str(name)] = str(plan_hash)
    return resolved


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
) -> pa.Table:
    backend = context.runtime.ibis_backend()
    table = read_delta_ibis(
        backend,
        str(path),
        options=IbisDeltaReadOptions(storage_options=context.storage.storage_options),
    )
    plan = IbisPlan(expr=table, ordering=Ordering.unordered())
    return materialize_ibis_plan(plan, execution=context.runtime.ibis_execution())


__all__ = [
    "InvalidationResult",
    "InvalidationSnapshot",
    "build_invalidation_snapshot",
    "check_state_store_invalidation",
    "diff_invalidation_snapshots",
    "read_invalidation_snapshot",
    "write_invalidation_snapshot",
]
