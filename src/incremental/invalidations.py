"""Invalidation gates for incremental state reuse."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.metadata import schema_identity_from_metadata
from incremental.state_store import StateStore
from relspec.rules.cache import rule_graph_signature_cached, rule_plan_signatures_cached
from schema_spec.system import SchemaRegistry


@dataclass(frozen=True)
class SchemaIdentity:
    """Schema identity metadata derived from Arrow schema metadata."""

    name: str | None
    version: int | None


@dataclass(frozen=True)
class InvalidationSnapshot:
    """Persisted signatures for invalidation checks."""

    rule_plan_signatures: dict[str, str]
    rule_graph_signature: str
    dataset_identities: dict[str, SchemaIdentity]

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-serializable payload for the snapshot.

        Returns
        -------
        dict[str, object]
            Payload suitable for JSON serialization.
        """
        return {
            "rule_plan_signatures": dict(self.rule_plan_signatures),
            "rule_graph_signature": self.rule_graph_signature,
            "dataset_identities": {
                name: {"name": identity.name, "version": identity.version}
                for name, identity in self.dataset_identities.items()
            },
        }


@dataclass(frozen=True)
class InvalidationResult:
    """Outcome for an invalidation check."""

    full_refresh: bool
    reasons: tuple[str, ...] = ()


def build_invalidation_snapshot(registry: SchemaRegistry) -> InvalidationSnapshot:
    """Build the current invalidation signature snapshot.

    Returns
    -------
    InvalidationSnapshot
        Snapshot of current signatures and schema identities.
    """
    return InvalidationSnapshot(
        rule_plan_signatures=rule_plan_signatures_cached(),
        rule_graph_signature=rule_graph_signature_cached(),
        dataset_identities=_dataset_identities(registry),
    )


def read_invalidation_snapshot(state_store: StateStore) -> InvalidationSnapshot | None:
    """Read the prior invalidation snapshot if present.

    Returns
    -------
    InvalidationSnapshot | None
        Snapshot when present, otherwise None.
    """
    path = state_store.invalidation_snapshot_path()
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return _snapshot_from_payload(payload)


def write_invalidation_snapshot(
    state_store: StateStore,
    snapshot: InvalidationSnapshot,
) -> None:
    """Persist the invalidation snapshot to the state store."""
    state_store.ensure_dirs()
    path = state_store.invalidation_snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = snapshot.to_payload()
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, sort_keys=True, indent=2)


def check_state_store_invalidation(
    *,
    state_store: StateStore,
    registry: SchemaRegistry,
) -> InvalidationResult:
    """Check invalidation signatures and reset state store on mismatch.

    Returns
    -------
    InvalidationResult
        Outcome indicating whether a full refresh is required.
    """
    current = build_invalidation_snapshot(registry)
    previous = read_invalidation_snapshot(state_store)
    if previous is None:
        write_invalidation_snapshot(state_store, current)
        return InvalidationResult(full_refresh=False, reasons=("missing_snapshot",))
    reasons = diff_invalidation_snapshots(previous, current)
    if reasons:
        reset_state_store(state_store)
        write_invalidation_snapshot(state_store, current)
        return InvalidationResult(full_refresh=True, reasons=reasons)
    return InvalidationResult(full_refresh=False, reasons=())


def diff_invalidation_snapshots(
    previous: InvalidationSnapshot,
    current: InvalidationSnapshot,
) -> tuple[str, ...]:
    """Return reasons for invalidation between snapshots.

    Returns
    -------
    tuple[str, ...]
        Reason codes describing invalidations.
    """
    reasons: list[str] = []
    if previous.rule_graph_signature != current.rule_graph_signature:
        reasons.append("rule_graph_signature")
    reasons.extend(
        _diff_mapping(
            previous.rule_plan_signatures,
            current.rule_plan_signatures,
            prefix="rule_signature",
        )
    )
    reasons.extend(
        _diff_schema_identities(previous.dataset_identities, current.dataset_identities)
    )
    return tuple(reasons)


def reset_state_store(state_store: StateStore) -> None:
    """Clear state store datasets and snapshots."""
    shutil.rmtree(state_store.datasets_dir(), ignore_errors=True)
    shutil.rmtree(state_store.snapshots_dir(), ignore_errors=True)


def validate_schema_identity(
    *,
    expected: SchemaLike,
    actual: SchemaLike,
    dataset_name: str,
) -> None:
    """Raise when schema identities do not match.

    Raises
    ------
    ValueError
        Raised when schema identity metadata does not match.
    """
    expected_name, expected_version = schema_identity_from_metadata(expected.metadata)
    actual_name, actual_version = schema_identity_from_metadata(actual.metadata)
    if expected_name != actual_name or expected_version != actual_version:
        msg = (
            "Schema identity mismatch for dataset "
            f"{dataset_name!r}: expected {expected_name}@{expected_version}, "
            f"got {actual_name}@{actual_version}."
        )
        raise ValueError(msg)


def _dataset_identities(registry: SchemaRegistry) -> dict[str, SchemaIdentity]:
    identities: dict[str, SchemaIdentity] = {}
    for name, spec in registry.dataset_specs.items():
        schema = spec.schema()
        meta_name, meta_version = schema_identity_from_metadata(schema.metadata)
        resolved_name = meta_name or spec.table_spec.name
        resolved_version = meta_version
        if resolved_version is None:
            resolved_version = spec.table_spec.version
        identities[name] = SchemaIdentity(
            name=resolved_name,
            version=resolved_version,
        )
    return identities


def _snapshot_from_payload(payload: object) -> InvalidationSnapshot:
    if not isinstance(payload, Mapping):
        msg = "Invalid invalidation snapshot payload."
        raise TypeError(msg)
    rule_signatures = _coerce_str_mapping(payload.get("rule_plan_signatures"))
    rule_graph_signature = str(payload.get("rule_graph_signature", ""))
    dataset_payload = payload.get("dataset_identities")
    dataset_identities = _coerce_schema_identities(dataset_payload)
    return InvalidationSnapshot(
        rule_plan_signatures=rule_signatures,
        rule_graph_signature=rule_graph_signature,
        dataset_identities=dataset_identities,
    )


def _coerce_str_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _coerce_schema_identities(value: object) -> dict[str, SchemaIdentity]:
    if not isinstance(value, Mapping):
        return {}
    identities: dict[str, SchemaIdentity] = {}
    for key, item in value.items():
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        version = _coerce_optional_int(item.get("version"))
        identities[str(key)] = SchemaIdentity(
            name=str(name) if name is not None else None,
            version=version,
        )
    return identities


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except ValueError:
        return None


def _diff_mapping(
    previous: Mapping[str, str],
    current: Mapping[str, str],
    *,
    prefix: str,
) -> list[str]:
    keys = set(previous) | set(current)
    return [
        f"{prefix}:{key}"
        for key in sorted(keys)
        if previous.get(key) != current.get(key)
    ]


def _diff_schema_identities(
    previous: Mapping[str, SchemaIdentity],
    current: Mapping[str, SchemaIdentity],
) -> list[str]:
    keys = set(previous) | set(current)
    return [
        f"schema_identity:{key}"
        for key in sorted(keys)
        if previous.get(key) != current.get(key)
    ]


__all__ = [
    "InvalidationResult",
    "InvalidationSnapshot",
    "SchemaIdentity",
    "build_invalidation_snapshot",
    "check_state_store_invalidation",
    "diff_invalidation_snapshots",
    "read_invalidation_snapshot",
    "reset_state_store",
    "validate_schema_identity",
    "write_invalidation_snapshot",
]
