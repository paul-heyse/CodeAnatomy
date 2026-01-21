"""Invalidation gates for incremental state reuse."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike
from arrowdsl.schema.build import table_from_arrays
from arrowdsl.schema.metadata import schema_identity_from_metadata
from datafusion_engine.runtime import read_delta_table_from_path
from ibis_engine.plan_diff import DiffOpEntry, semantic_diff_sql
from incremental.state_store import StateStore
from registry_common.arrow_payloads import payload_hash
from relspec.graph import rule_graph_signature
from relspec.registry.snapshot import RelspecSnapshot
from relspec.rules.cache import (
    relspec_snapshot_cached,
    rule_definitions_cached,
    rule_graph_signature_cached,
    rule_plan_sql_cached,
)
from relspec.schema_context import RelspecSchemaContext
from storage.deltalake import (
    DeltaWriteOptions,
    enable_delta_features,
    write_table_delta,
)

INVALIDATION_SNAPSHOT_VERSION = 1
_RULE_SIGNATURE_ENTRY = pa.struct(
    [
        pa.field("rule_name", pa.string(), nullable=False),
        pa.field("signature", pa.string(), nullable=False),
    ]
)
_RULE_SQL_ENTRY = pa.struct(
    [
        pa.field("rule_name", pa.string(), nullable=False),
        pa.field("sql", pa.string(), nullable=False),
    ]
)
_DATASET_IDENTITY_ENTRY = pa.struct(
    [
        pa.field("dataset_name", pa.string(), nullable=False),
        pa.field("schema_name", pa.string(), nullable=True),
        pa.field("schema_version", pa.int64(), nullable=True),
    ]
)
_INVALIDATION_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("rule_graph_signature", pa.string(), nullable=False),
        pa.field("rule_plan_signatures", pa.list_(_RULE_SIGNATURE_ENTRY), nullable=False),
        pa.field("rule_plan_sql", pa.list_(_RULE_SQL_ENTRY), nullable=False),
        pa.field("dataset_identities", pa.list_(_DATASET_IDENTITY_ENTRY), nullable=False),
    ]
)
_SQL_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("sql", pa.string(), nullable=False),
    ]
)


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
        """Return an Arrow-friendly payload for the snapshot.

        Returns
        -------
        dict[str, object]
            Payload suitable for IPC serialization.
        """
        return {
            "version": INVALIDATION_SNAPSHOT_VERSION,
            "rule_graph_signature": self.rule_graph_signature,
            "rule_plan_signatures": _rule_entries(self.rule_plan_signatures),
            "rule_plan_sql": _rule_sql_entries(self.rule_plan_sql),
            "dataset_identities": _dataset_entries(self.dataset_identities),
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


def build_invalidation_snapshot(
    schema_context: RelspecSchemaContext,
    *,
    relspec_snapshot: RelspecSnapshot | None = None,
) -> InvalidationSnapshot:
    """Build the current invalidation signature snapshot.

    Returns
    -------
    InvalidationSnapshot
        Snapshot of current signatures and schema identities.
    """
    snapshot = relspec_snapshot or relspec_snapshot_cached()
    if relspec_snapshot is None:
        rule_graph_signature = rule_graph_signature_cached()
    else:
        rule_graph_signature = _rule_graph_signature_for_snapshot(snapshot)
    return InvalidationSnapshot(
        rule_plan_signatures=dict(snapshot.plan_signatures),
        rule_plan_sql=rule_plan_sql_cached(),
        rule_graph_signature=rule_graph_signature,
        dataset_identities=_dataset_identities(schema_context),
    )


def read_invalidation_snapshot(state_store: StateStore) -> InvalidationSnapshot | None:
    """Read the prior invalidation snapshot if present.

    Returns
    -------
    InvalidationSnapshot | None
        Snapshot when present, otherwise None.

    Raises
    ------
    TypeError
        Raised when the snapshot payload is not a mapping.
    """
    path = state_store.invalidation_snapshot_path()
    if not path.exists():
        return None
    table = read_delta_table_from_path(str(path))
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
) -> None:
    """Persist the invalidation snapshot to the state store."""
    state_store.ensure_dirs()
    path = state_store.invalidation_snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = snapshot.to_payload()
    table = pa.Table.from_pylist([payload], schema=_INVALIDATION_SCHEMA)
    result = write_table_delta(
        table,
        str(path),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={"snapshot_kind": "invalidation_snapshot"},
        ),
    )
    enable_delta_features(result.path)


def check_state_store_invalidation(
    *,
    state_store: StateStore,
    schema_context: RelspecSchemaContext,
    relspec_snapshot: RelspecSnapshot | None = None,
) -> InvalidationResult:
    """Check invalidation signatures and reset state store on mismatch.

    Returns
    -------
    InvalidationResult
        Outcome indicating whether a full refresh is required.
    """
    current = build_invalidation_snapshot(schema_context, relspec_snapshot=relspec_snapshot)
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
    schema_context: RelspecSchemaContext,
    relspec_snapshot: RelspecSnapshot | None = None,
) -> InvalidationOutcome:
    """Check invalidation signatures and return plan diff details.

    Returns
    -------
    InvalidationOutcome
        Outcome containing invalidation result and plan diff table.
    """
    current = build_invalidation_snapshot(schema_context, relspec_snapshot=relspec_snapshot)
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


def _dataset_identities(schema_context: RelspecSchemaContext) -> dict[str, SchemaIdentity]:
    identities: dict[str, SchemaIdentity] = {}
    for name in schema_context.dataset_names():
        schema = schema_context.dataset_schema(name)
        if schema is None:
            continue
        meta_name, meta_version = schema_identity_from_metadata(schema.metadata)
        resolved_name = meta_name or name
        identities[name] = SchemaIdentity(
            name=resolved_name,
            version=meta_version,
        )
    return identities


def _rule_graph_signature_for_snapshot(snapshot: RelspecSnapshot) -> str:
    rules = rule_definitions_cached()
    return rule_graph_signature(
        rules,
        name_for=lambda rule: rule.name,
        signature_for=lambda rule: snapshot.plan_signatures.get(rule.name, ""),
        label="all",
    )


def _snapshot_from_row(row: Mapping[str, object]) -> InvalidationSnapshot:
    version = row.get("version")
    if version is not None:
        if isinstance(version, int) and not isinstance(version, bool):
            version_value = version
        elif isinstance(version, (str, bytes, bytearray)):
            version_value = int(version)
        else:
            msg = "Invalid invalidation snapshot version."
            raise TypeError(msg)
        if version_value != INVALIDATION_SNAPSHOT_VERSION:
            msg = "Invalid invalidation snapshot version."
            raise ValueError(msg)
    rule_signatures = _coerce_rule_entries(row.get("rule_plan_signatures"))
    rule_plan_sql = _coerce_rule_sql_entries(row.get("rule_plan_sql"))
    rule_graph_signature = str(row.get("rule_graph_signature", ""))
    dataset_identities = _coerce_dataset_entries(row.get("dataset_identities"))
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
    payload = {"version": INVALIDATION_SNAPSHOT_VERSION, "sql": value}
    return payload_hash(payload, _SQL_HASH_SCHEMA)


def _edit_script_payload(script: tuple[DiffOpEntry, ...]) -> str | None:
    if not script:
        return None
    payload = [entry.payload() for entry in script]
    return _stable_repr(payload)


def _rule_entries(values: Mapping[str, str]) -> list[dict[str, str]]:
    return [
        {"rule_name": name, "signature": signature} for name, signature in sorted(values.items())
    ]


def _rule_sql_entries(values: Mapping[str, str]) -> list[dict[str, str]]:
    return [{"rule_name": name, "sql": sql} for name, sql in sorted(values.items())]


def _dataset_entries(values: Mapping[str, SchemaIdentity]) -> list[dict[str, object]]:
    return [
        {
            "dataset_name": name,
            "schema_name": identity.name,
            "schema_version": identity.version,
        }
        for name, identity in sorted(values.items(), key=lambda item: item[0])
    ]


def _coerce_rule_entries(value: object) -> dict[str, str]:
    entries = _coerce_entry_list(value, label="rule_plan_signatures")
    resolved: dict[str, str] = {}
    for entry in entries:
        name = entry.get("rule_name")
        signature = entry.get("signature")
        if name is None or signature is None:
            continue
        resolved[str(name)] = str(signature)
    return resolved


def _coerce_rule_sql_entries(value: object) -> dict[str, str]:
    entries = _coerce_entry_list(value, label="rule_plan_sql")
    resolved: dict[str, str] = {}
    for entry in entries:
        name = entry.get("rule_name")
        sql = entry.get("sql")
        if name is None or sql is None:
            continue
        resolved[str(name)] = str(sql)
    return resolved


def _coerce_dataset_entries(value: object) -> dict[str, SchemaIdentity]:
    entries = _coerce_entry_list(value, label="dataset_identities")
    resolved: dict[str, SchemaIdentity] = {}
    for entry in entries:
        dataset_name = entry.get("dataset_name")
        if dataset_name is None:
            continue
        name = entry.get("schema_name")
        version = _coerce_optional_int(entry.get("schema_version"))
        resolved[str(dataset_name)] = SchemaIdentity(
            name=str(name) if name is not None else None,
            version=version,
        )
    return resolved


def _coerce_entry_list(value: object, *, label: str) -> list[Mapping[str, object]]:
    if value is None:
        return []
    if isinstance(value, list):
        entries: list[Mapping[str, object]] = []
        for item in value:
            if isinstance(item, Mapping):
                entries.append(item)
            else:
                msg = f"{label} entries must be mappings."
                raise TypeError(msg)
        return entries
    msg = f"{label} must be a list of mappings."
    raise TypeError(msg)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


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
