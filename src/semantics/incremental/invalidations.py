"""Invalidation helpers for incremental state snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa

from datafusion_engine.arrow.schema import (
    plan_fingerprint_entry_type,
    version_field,
)
from datafusion_engine.io.write import WriteMode
from engine.runtime_profile import runtime_profile_snapshot
from semantics.incremental.delta_context import read_delta_table_via_facade
from semantics.incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)
from serde_artifacts import ViewArtifactPayload
from serde_msgspec import convert, validation_error_payload
from storage.deltalake import delta_table_version
from storage.ipc_utils import payload_hash

INVALIDATION_SNAPSHOT_VERSION = 2
_PLAN_FINGERPRINT_ENTRY = plan_fingerprint_entry_type()
_INVALIDATION_SCHEMA = pa.schema(
    [
        version_field(),
        pa.field(
            "incremental_plan_fingerprints",
            pa.list_(_PLAN_FINGERPRINT_ENTRY),
            nullable=False,
        ),
        pa.field("incremental_metadata_hash", pa.string(), nullable=True),
        pa.field("runtime_profile_hash", pa.string(), nullable=True),
    ]
)
_INCREMENTAL_METADATA_SCHEMA = pa.schema(
    [
        pa.field("datafusion_settings_hash", pa.string(), nullable=False),
        pa.field("runtime_profile_hash", pa.string(), nullable=False),
    ]
)

if TYPE_CHECKING:
    from semantics.incremental.delta_context import DeltaAccessContext
    from semantics.incremental.runtime import IncrementalRuntime
    from semantics.incremental.state_store import StateStore


@dataclass(frozen=True)
class PlanFingerprint:
    """Fingerprint fields for a compiled plan."""

    plan_fingerprint: str
    plan_task_signature: str = ""


@dataclass(frozen=True)
class InvalidationSnapshot:
    """Persisted signatures for incremental invalidation checks."""

    incremental_plan_fingerprints: dict[str, PlanFingerprint]
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
            "incremental_plan_fingerprints": _plan_fingerprint_entries(
                self.incremental_plan_fingerprints
            ),
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
        Snapshot of plan fingerprints + runtime metadata.
    """
    runtime = context.runtime
    return InvalidationSnapshot(
        incremental_plan_fingerprints=_incremental_plan_fingerprints(
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
    resolved = context.resolve_storage(table_uri=str(path))
    version = delta_table_version(
        str(path),
        storage_options=resolved.storage_options,
        log_storage_options=resolved.log_storage_options,
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
    resolved_storage = context.resolve_storage(table_uri=str(path))
    write_delta_table_via_pipeline(
        runtime=context.runtime,
        table=table,
        request=IncrementalDeltaWriteRequest(
            destination=str(path),
            mode=WriteMode.OVERWRITE,
            schema_mode="overwrite",
            commit_metadata={"snapshot_kind": "incremental_invalidation_snapshot"},
            storage_options=resolved_storage.storage_options,
            log_storage_options=resolved_storage.log_storage_options,
            operation_id="incremental_invalidation_snapshot",
        ),
    )
    return str(path)


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
            previous.incremental_plan_fingerprints,
            current.incremental_plan_fingerprints,
            prefix="incremental_plan_fingerprint",
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
    runtime_snapshot = runtime_profile_snapshot(
        runtime.profile,
        name=runtime.profile.config_policy_name,
        determinism_tier=runtime.determinism_tier,
    )
    payload = {
        "datafusion_settings_hash": runtime.profile.settings_hash(),
        "runtime_profile_hash": runtime_snapshot.profile_hash,
    }
    return payload_hash(payload, _INCREMENTAL_METADATA_SCHEMA)


def _runtime_profile_hash(runtime: IncrementalRuntime) -> str | None:
    return runtime_profile_snapshot(
        runtime.profile,
        name=runtime.profile.config_policy_name,
        determinism_tier=runtime.determinism_tier,
    ).profile_hash


def _incremental_plan_fingerprints(
    *,
    context: DeltaAccessContext,
    state_store: StateStore | None,
) -> dict[str, PlanFingerprint]:
    runtime = context.runtime
    artifacts = runtime.profile.view_registry_snapshot()
    if artifacts:
        return _plan_fingerprints_from_artifacts(artifacts)
    if state_store is None:
        return {}
    path = state_store.view_artifacts_path()
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
    table = _read_delta_table(context, path)
    rows = table.to_pylist()
    return _plan_fingerprints_from_artifacts(rows)


def _plan_fingerprints_from_artifacts(
    artifacts: Sequence[Mapping[str, object]],
) -> dict[str, PlanFingerprint]:
    resolved: dict[str, PlanFingerprint] = {}
    for payload in artifacts:
        payload_raw = dict(payload)
        if not payload_raw.get("plan_task_signature") and payload_raw.get("plan_fingerprint"):
            payload_raw["plan_task_signature"] = payload_raw["plan_fingerprint"]
        try:
            artifact = convert(payload_raw, target_type=ViewArtifactPayload, strict=True)
        except msgspec.ValidationError as exc:
            details = validation_error_payload(exc)
            msg = f"View artifact payload validation failed: {details}"
            raise ValueError(msg) from exc
        signature_value = artifact.plan_task_signature or artifact.plan_fingerprint
        resolved[artifact.name] = PlanFingerprint(
            plan_fingerprint=artifact.plan_fingerprint,
            plan_task_signature=signature_value,
        )
    return resolved


def _plan_fingerprint_entries(
    plan_fingerprints: Mapping[str, PlanFingerprint],
) -> list[dict[str, object]]:
    return [
        {
            "plan_name": name,
            "plan_fingerprint": fingerprint.plan_fingerprint,
            "plan_task_signature": fingerprint.plan_task_signature or fingerprint.plan_fingerprint,
        }
        for name, fingerprint in sorted(plan_fingerprints.items())
    ]


def _diff_mapping(
    previous: Mapping[str, PlanFingerprint],
    current: Mapping[str, PlanFingerprint],
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
    plan_fingerprints = _parse_plan_fingerprint_entries(row.get("incremental_plan_fingerprints"))
    metadata_hash = row.get("incremental_metadata_hash")
    runtime_hash = row.get("runtime_profile_hash")
    return InvalidationSnapshot(
        incremental_plan_fingerprints=plan_fingerprints,
        incremental_metadata_hash=str(metadata_hash) if metadata_hash is not None else None,
        runtime_profile_hash=str(runtime_hash) if runtime_hash is not None else None,
    )


def _parse_plan_fingerprint_entries(value: object) -> dict[str, PlanFingerprint]:
    if not isinstance(value, Sequence):
        return {}
    resolved: dict[str, PlanFingerprint] = {}
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        name = entry.get("plan_name")
        plan_fingerprint = entry.get("plan_fingerprint")
        plan_task_signature = entry.get("plan_task_signature")
        if name is None or plan_fingerprint is None:
            continue
        signature_value = str(plan_task_signature) if plan_task_signature else str(plan_fingerprint)
        resolved[str(name)] = PlanFingerprint(
            plan_fingerprint=str(plan_fingerprint),
            plan_task_signature=signature_value,
        )
    return resolved


def _read_delta_table(
    context: DeltaAccessContext,
    path: Path,
) -> pa.Table:
    return read_delta_table_via_facade(
        context,
        path=path,
        name="invalidations_read",
    )


__all__ = [
    "InvalidationResult",
    "InvalidationSnapshot",
    "build_invalidation_snapshot",
    "check_state_store_invalidation",
    "diff_invalidation_snapshots",
    "read_invalidation_snapshot",
    "write_invalidation_snapshot",
]
