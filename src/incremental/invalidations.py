"""Invalidation gates for incremental state reuse."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.metadata import schema_identity_from_metadata
from ibis_engine.plan_diff import DiffOpEntry, semantic_diff_sql
from incremental.state_store import StateStore
from relspec.rules.cache import (
    rule_graph_signature_cached,
    rule_plan_signatures_cached,
    rule_plan_sql_cached,
)
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
    rule_plan_sql: dict[str, str]
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
            "rule_plan_sql": dict(self.rule_plan_sql),
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


@dataclass(frozen=True)
class InvalidationOutcome:
    """Outcome plus plan diff for invalidation checks."""

    result: InvalidationResult
    plan_diff: pa.Table


def build_invalidation_snapshot(registry: SchemaRegistry) -> InvalidationSnapshot:
    """Build the current invalidation signature snapshot.

    Returns
    -------
    InvalidationSnapshot
        Snapshot of current signatures and schema identities.
    """
    return InvalidationSnapshot(
        rule_plan_signatures=rule_plan_signatures_cached(),
        rule_plan_sql=rule_plan_sql_cached(),
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


def check_state_store_invalidation_with_diff(
    *,
    state_store: StateStore,
    registry: SchemaRegistry,
) -> InvalidationOutcome:
    """Check invalidation signatures and return plan diff details.

    Returns
    -------
    InvalidationOutcome
        Outcome containing invalidation result and plan diff table.
    """
    current = build_invalidation_snapshot(registry)
    previous = read_invalidation_snapshot(state_store)
    diff_table = rule_plan_diff_table(previous, current)
    if previous is None:
        write_invalidation_snapshot(state_store, current)
        result = InvalidationResult(full_refresh=False, reasons=("missing_snapshot",))
        return InvalidationOutcome(result=result, plan_diff=diff_table)
    reasons = diff_invalidation_snapshots(previous, current)
    if reasons:
        reset_state_store(state_store)
        write_invalidation_snapshot(state_store, current)
        return InvalidationOutcome(
            result=InvalidationResult(full_refresh=True, reasons=reasons),
            plan_diff=diff_table,
        )
    write_invalidation_snapshot(state_store, current)
    return InvalidationOutcome(
        result=InvalidationResult(full_refresh=False, reasons=()),
        plan_diff=diff_table,
    )


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
    reasons.extend(_diff_rule_plan_sql(previous.rule_plan_sql, current.rule_plan_sql))
    if not previous.rule_plan_sql or not current.rule_plan_sql:
        reasons.extend(
            _diff_mapping(
                previous.rule_plan_signatures,
                current.rule_plan_signatures,
                prefix="rule_signature",
            )
        )
    reasons.extend(_diff_schema_identities(previous.dataset_identities, current.dataset_identities))
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
    rule_plan_sql = _coerce_str_mapping(payload.get("rule_plan_sql"))
    rule_graph_signature = str(payload.get("rule_graph_signature", ""))
    dataset_payload = payload.get("dataset_identities")
    dataset_identities = _coerce_schema_identities(dataset_payload)
    return InvalidationSnapshot(
        rule_plan_signatures=rule_signatures,
        rule_plan_sql=rule_plan_sql,
        rule_graph_signature=rule_graph_signature,
        dataset_identities=dataset_identities,
    )


def _diff_rule_plan_sql(
    previous: Mapping[str, str],
    current: Mapping[str, str],
) -> list[str]:
    reasons: list[str] = []
    all_rules = sorted(set(previous) | set(current))
    for name in all_rules:
        prev_sql = previous.get(name)
        next_sql = current.get(name)
        if not prev_sql or not next_sql:
            reasons.append(f"rule_plan_sql:{name}")
            continue
        diff = semantic_diff_sql(prev_sql, next_sql)
        if diff.changed:
            reasons.append(f"rule_plan_sql:{name}")
    return reasons


def rule_plan_diff_table(
    previous: InvalidationSnapshot | None,
    current: InvalidationSnapshot,
) -> pa.Table:
    """Return a table describing rule plan SQL changes.

    Returns
    -------
    pyarrow.Table
        Table of plan diff details per rule.
    """
    schema = pa.schema(
        [
            ("rule_name", pa.string()),
            ("change_kind", pa.string()),
            ("diff_types", pa.string()),
            ("diff_breaking", pa.bool_()),
            ("diff_script", pa.string()),
            ("prev_plan_hash", pa.string()),
            ("cur_plan_hash", pa.string()),
        ]
    )
    prev_sql: dict[str, str] = dict(previous.rule_plan_sql) if previous is not None else {}
    cur_sql = current.rule_plan_sql
    names = sorted(set(prev_sql) | set(cur_sql))
    if not names:
        return table_from_arrays(schema, columns={}, num_rows=0)
    change_kind: list[str] = []
    diff_types: list[str | None] = []
    diff_breaking: list[bool | None] = []
    diff_script: list[str | None] = []
    prev_hashes: list[str | None] = []
    cur_hashes: list[str | None] = []
    for name in names:
        prev = prev_sql.get(name)
        cur = cur_sql.get(name)
        prev_hashes.append(_hash_sql(prev))
        cur_hashes.append(_hash_sql(cur))
        if prev is None and cur is not None:
            change_kind.append("added")
            diff_types.append(None)
            diff_breaking.append(True)
            diff_script.append(None)
            continue
        if prev is not None and cur is None:
            change_kind.append("removed")
            diff_types.append(None)
            diff_breaking.append(True)
            diff_script.append(None)
            continue
        if prev is None or cur is None:
            change_kind.append("changed")
            diff_types.append("missing")
            diff_breaking.append(True)
            diff_script.append(None)
            continue
        diff = semantic_diff_sql(prev, cur)
        if diff.changed:
            change_kind.append("changed")
            diff_types.append(",".join(diff.changes))
            diff_breaking.append(diff.breaking)
            diff_script.append(_edit_script_payload(diff.edit_script))
        else:
            change_kind.append("unchanged")
            diff_types.append(None)
            diff_breaking.append(False)
            diff_script.append(None)
    return table_from_arrays(
        schema,
        columns={
            "rule_name": pa.array(names, type=pa.string()),
            "change_kind": pa.array(change_kind, type=pa.string()),
            "diff_types": pa.array(diff_types, type=pa.string()),
            "diff_breaking": pa.array(diff_breaking, type=pa.bool_()),
            "diff_script": pa.array(diff_script, type=pa.string()),
            "prev_plan_hash": pa.array(prev_hashes, type=pa.string()),
            "cur_plan_hash": pa.array(cur_hashes, type=pa.string()),
        },
        num_rows=len(names),
    )


def _hash_sql(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _edit_script_payload(script: tuple[DiffOpEntry, ...]) -> str | None:
    if not script:
        return None
    payload = [entry.payload() for entry in script]
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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
    return [f"{prefix}:{key}" for key in sorted(keys) if previous.get(key) != current.get(key)]


def _diff_schema_identities(
    previous: Mapping[str, SchemaIdentity],
    current: Mapping[str, SchemaIdentity],
) -> list[str]:
    keys = set(previous) | set(current)
    return [
        f"schema_identity:{key}" for key in sorted(keys) if previous.get(key) != current.get(key)
    ]


__all__ = [
    "InvalidationOutcome",
    "InvalidationResult",
    "InvalidationSnapshot",
    "SchemaIdentity",
    "build_invalidation_snapshot",
    "check_state_store_invalidation",
    "check_state_store_invalidation_with_diff",
    "diff_invalidation_snapshots",
    "read_invalidation_snapshot",
    "reset_state_store",
    "rule_plan_diff_table",
    "validate_schema_identity",
    "write_invalidation_snapshot",
]
