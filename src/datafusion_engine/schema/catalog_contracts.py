"""Schema contracts derived from information_schema metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.schema.contracts import SchemaContract
from datafusion_engine.schema.introspection import SchemaIntrospector
from serde_msgspec import dumps_msgpack
from validation.violations import ValidationViolation, ViolationType

INFO_SCHEMA_COLUMNS_META: bytes = b"information_schema_columns"
INFO_SCHEMA_CONSTRAINTS_META: bytes = b"information_schema_constraints"


@dataclass(frozen=True)
class InformationSchemaSnapshot:
    """Information schema snapshot payloads for a single table."""

    columns: tuple[Mapping[str, object], ...]
    constraints: tuple[str, ...]


def information_schema_snapshot(
    ctx: SessionContext,
    *,
    table_name: str,
) -> InformationSchemaSnapshot:
    """Return information_schema snapshots for a table.

    Returns:
    -------
    InformationSchemaSnapshot
        Snapshot of information_schema columns and constraints.
    """
    introspector = SchemaIntrospector(ctx)
    columns = tuple(introspector.table_columns_with_ordinal(table_name))
    constraints = tuple(introspector.table_constraints(table_name))
    return InformationSchemaSnapshot(columns=columns, constraints=constraints)


def schema_contract_from_information_schema(
    ctx: SessionContext,
    *,
    table_name: str,
) -> SchemaContract:
    """Build a SchemaContract from information_schema metadata.

    Args:
        ctx: DataFusion session context.
        table_name: Registered table name.

    Returns:
        SchemaContract: Result.

    Raises:
        ValueError: If information schema columns are unavailable.
    """
    snapshot = information_schema_snapshot(ctx, table_name=table_name)
    if not snapshot.columns:
        msg = f"information_schema columns unavailable for {table_name!r}."
        raise ValueError(msg)
    introspector = SchemaIntrospector(ctx)
    arrow_schema = introspector.table_schema(table_name)
    ordered_schema = _ordered_schema_from_columns(arrow_schema, snapshot.columns)
    metadata = dict(ordered_schema.metadata or {})
    metadata[INFO_SCHEMA_COLUMNS_META] = dumps_msgpack(list(snapshot.columns))
    metadata[INFO_SCHEMA_CONSTRAINTS_META] = dumps_msgpack(list(snapshot.constraints))
    return SchemaContract.from_arrow_schema(
        table_name,
        ordered_schema,
        schema_metadata=metadata,
    )


def contract_violations_for_schema(
    *,
    contract: SchemaContract,
    schema: pa.Schema,
) -> list[ValidationViolation]:
    """Return violations comparing an Arrow schema to a SchemaContract.

    Returns:
    -------
    list[ValidationViolation]
        Violations detected when comparing the schema to the contract.
    """
    expected_fields = {field.name: field for field in contract.columns}
    actual_fields = {field.name: field for field in schema}
    violations: list[ValidationViolation] = []
    for name, expected in expected_fields.items():
        actual = actual_fields.get(name)
        if actual is None:
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.MISSING_COLUMN,
                    table_name=contract.table_name,
                    column_name=name,
                    expected=str(expected.dtype),
                    actual=None,
                )
            )
            continue
        if actual.type != expected.dtype:
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.TYPE_MISMATCH,
                    table_name=contract.table_name,
                    column_name=name,
                    expected=str(expected.dtype),
                    actual=str(actual.type),
                )
            )
        if actual.nullable != expected.nullable:
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.NULLABILITY_MISMATCH,
                    table_name=contract.table_name,
                    column_name=name,
                    expected=str(expected.nullable),
                    actual=str(actual.nullable),
                )
            )
    extra = sorted(name for name in actual_fields if name not in expected_fields)
    violations.extend(
        ValidationViolation(
            violation_type=ViolationType.EXTRA_COLUMN,
            table_name=contract.table_name,
            column_name=name,
            expected=None,
            actual=str(schema.field(name).type),
        )
        for name in extra
    )
    return violations


def _ordered_schema_from_columns(
    schema: pa.Schema,
    columns: Iterable[Mapping[str, object]],
) -> pa.Schema:
    names = _column_names(columns)
    if not names:
        return schema
    actual_names = [field.name for field in schema]
    missing = [name for name in names if name not in actual_names]
    extra = [name for name in actual_names if name not in names]
    if missing:
        msg = f"information_schema missing columns in schema: {missing}."
        raise ValueError(msg)
    if extra:
        msg = f"information_schema missing schema columns: {extra}."
        raise ValueError(msg)
    ordered_fields = [schema.field(name) for name in names]
    return pa.schema(ordered_fields, metadata=schema.metadata)


def _column_names(columns: Iterable[Mapping[str, object]]) -> list[str]:
    return [
        name
        for row in columns
        if (value := row.get("column_name")) is not None
        if (name := str(value))
    ]


__all__ = [
    "INFO_SCHEMA_COLUMNS_META",
    "INFO_SCHEMA_CONSTRAINTS_META",
    "InformationSchemaSnapshot",
    "contract_violations_for_schema",
    "information_schema_snapshot",
    "schema_contract_from_information_schema",
]
