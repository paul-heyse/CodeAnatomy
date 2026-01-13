"""Arrow spec tables for schema specifications."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, TypedDict, cast

import pyarrow as pa
from pyarrow import ipc

import schema_spec.system as schema_system
from arrowdsl.core.interop import DataTypeLike
from arrowdsl.schema.validation import ArrowValidationOptions
from schema_spec.specs import (
    DICT_STRING,
    ArrowFieldSpec,
    TableSchemaSpec,
)

FIELD_SPEC_SCHEMA = pa.schema(
    [
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("table_version", pa.int64(), nullable=True),
        pa.field("field_name", pa.string(), nullable=False),
        pa.field("field_type", pa.binary(), nullable=False),
        pa.field("nullable", pa.bool_(), nullable=False),
        pa.field("encoding", DICT_STRING, nullable=True),
        pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"schema_fields"},
)

TABLE_CONSTRAINTS_SCHEMA = pa.schema(
    [
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("table_version", pa.int64(), nullable=True),
        pa.field("required_non_null", pa.list_(pa.string()), nullable=True),
        pa.field("key_fields", pa.list_(pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"schema_constraints"},
)

SORT_KEY_STRUCT = pa.struct(
    [
        pa.field("column", pa.string(), nullable=False),
        pa.field("order", pa.string(), nullable=False),
    ]
)

DEDUPE_STRUCT = pa.struct(
    [
        pa.field("keys", pa.list_(pa.string()), nullable=False),
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("strategy", pa.string(), nullable=False),
    ]
)

VALIDATION_STRUCT = pa.struct(
    [
        pa.field("strict", pa.string(), nullable=False),
        pa.field("coerce", pa.bool_(), nullable=False),
        pa.field("max_errors", pa.int64(), nullable=True),
        pa.field("emit_invalid_rows", pa.bool_(), nullable=False),
        pa.field("emit_error_table", pa.bool_(), nullable=False),
    ]
)

CONTRACT_SPEC_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("version", pa.int64(), nullable=True),
        pa.field("dedupe", DEDUPE_STRUCT, nullable=True),
        pa.field("canonical_sort", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("virtual_fields", pa.list_(pa.string()), nullable=True),
        pa.field("virtual_field_docs", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("validation", VALIDATION_STRUCT, nullable=True),
    ],
    metadata={b"spec_kind": b"contract_specs"},
)


@dataclass(frozen=True)
class SchemaSpecTables:
    """Bundle of schema spec tables."""

    field_table: pa.Table
    constraints_table: pa.Table | None = None
    contract_table: pa.Table | None = None


def _encode_dtype(dtype: DataTypeLike) -> bytes:
    schema = pa.schema([pa.field("field", dtype)])
    return schema.serialize().to_pybytes()


def _decode_dtype(payload: bytes) -> pa.DataType:
    reader = pa.BufferReader(payload)
    schema = ipc.read_schema(reader)
    return schema.field(0).type


def _encode_strict(*, strict: bool | Literal["filter"]) -> str:
    if strict is True:
        return "true"
    if strict is False:
        return "false"
    return "filter"


def _decode_strict(value: str) -> bool | Literal["filter"]:
    normalized = value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    if normalized == "filter":
        return "filter"
    msg = f"Unsupported strict value: {value!r}"
    raise ValueError(msg)


class ContractRow(TypedDict, total=False):
    name: str
    table_name: str
    version: int | None
    dedupe: dict[str, object] | None
    canonical_sort: list[dict[str, object]] | None
    virtual_fields: list[str] | None
    virtual_field_docs: dict[str, str] | None
    validation: dict[str, object] | None


def field_spec_table(specs: Sequence[TableSchemaSpec]) -> pa.Table:
    """Build a field spec table for table schema specs.

    Returns
    -------
    pa.Table
        Arrow table of field specs.
    """
    rows: list[dict[str, object]] = []
    for spec in specs:
        rows.extend(
            [
                {
                    "table_name": spec.name,
                    "table_version": spec.version,
                    "field_name": field_spec.name,
                    "field_type": _encode_dtype(field_spec.dtype),
                    "nullable": field_spec.nullable,
                    "encoding": field_spec.encoding,
                    "metadata": dict(field_spec.metadata) if field_spec.metadata else None,
                }
                for field_spec in spec.fields
            ]
        )
    return pa.Table.from_pylist(rows, schema=FIELD_SPEC_SCHEMA)


def table_constraints_table(specs: Sequence[TableSchemaSpec]) -> pa.Table:
    """Build a constraints table for table schema specs.

    Returns
    -------
    pa.Table
        Arrow table of schema constraints.
    """
    rows: list[dict[str, object]] = [
        {
            "table_name": spec.name,
            "table_version": spec.version,
            "required_non_null": list(spec.required_non_null) or None,
            "key_fields": list(spec.key_fields) or None,
        }
        for spec in specs
    ]
    return pa.Table.from_pylist(rows, schema=TABLE_CONSTRAINTS_SCHEMA)


def _sort_key_row(spec: schema_system.SortKeySpec) -> dict[str, object]:
    return {"column": spec.column, "order": spec.order}


def _dedupe_row(
    dedupe: schema_system.DedupeSpecSpec | None,
) -> dict[str, object] | None:
    if dedupe is None:
        return None
    return {
        "keys": list(dedupe.keys),
        "tie_breakers": [_sort_key_row(spec) for spec in dedupe.tie_breakers] or None,
        "strategy": dedupe.strategy,
    }


def _validation_row(options: ArrowValidationOptions | None) -> dict[str, object] | None:
    if options is None:
        return None
    return {
        "strict": _encode_strict(strict=options.strict),
        "coerce": options.coerce,
        "max_errors": options.max_errors,
        "emit_invalid_rows": options.emit_invalid_rows,
        "emit_error_table": options.emit_error_table,
    }


def contract_spec_table(specs: Sequence[schema_system.ContractSpec]) -> pa.Table:
    """Build a contract spec table.

    Returns
    -------
    pa.Table
        Arrow table of contract specs.
    """
    rows = [
        {
            "name": spec.name,
            "table_name": spec.table_schema.name,
            "version": spec.version,
            "dedupe": _dedupe_row(spec.dedupe),
            "canonical_sort": [_sort_key_row(sk) for sk in spec.canonical_sort] or None,
            "virtual_fields": list(spec.virtual_fields) or None,
            "virtual_field_docs": (
                dict(spec.virtual_field_docs) if spec.virtual_field_docs else None
            ),
            "validation": _validation_row(spec.validation),
        }
        for spec in specs
    ]
    return pa.Table.from_pylist(rows, schema=CONTRACT_SPEC_SCHEMA)


def table_specs_from_tables(
    field_table: pa.Table,
    constraints_table: pa.Table | None = None,
) -> dict[str, TableSchemaSpec]:
    """Compile TableSchemaSpecs from field and constraint tables.

    Returns
    -------
    dict[str, TableSchemaSpec]
        Mapping of table name to schema spec.
    """
    fields_by_table: dict[str, list[ArrowFieldSpec]] = {}
    versions: dict[str, int | None] = {}
    for row in field_table.to_pylist():
        name = str(row["table_name"])
        field_spec = ArrowFieldSpec(
            name=str(row["field_name"]),
            dtype=_decode_dtype(row["field_type"]),
            nullable=bool(row["nullable"]),
            metadata=dict(row["metadata"] or {}),
            encoding=row.get("encoding"),
        )
        fields_by_table.setdefault(name, []).append(field_spec)
        if name not in versions:
            versions[name] = row.get("table_version")

    constraints: dict[str, dict[str, Iterable[str]]] = {}
    if constraints_table is not None:
        for row in constraints_table.to_pylist():
            name = str(row["table_name"])
            versions[name] = row.get("table_version")
            constraints[name] = {
                "required_non_null": tuple(row.get("required_non_null") or ()),
                "key_fields": tuple(row.get("key_fields") or ()),
            }

    specs: dict[str, TableSchemaSpec] = {}
    for name, fields in fields_by_table.items():
        entry = constraints.get(name, {})
        specs[name] = TableSchemaSpec(
            name=name,
            version=versions.get(name),
            fields=fields,
            required_non_null=tuple(entry.get("required_non_null", ())),
            key_fields=tuple(entry.get("key_fields", ())),
        )
    return specs


def _sort_key_from_row(payload: Mapping[str, object]) -> schema_system.SortKeySpec:
    return schema_system.SortKeySpec(
        column=str(payload["column"]),
        order=_parse_sort_order(payload.get("order")),
    )


def _parse_sort_order(value: object) -> Literal["ascending", "descending"]:
    if value is None:
        return "ascending"
    normalized = str(value).lower()
    orders: dict[str, Literal["ascending", "descending"]] = {
        "ascending": "ascending",
        "descending": "descending",
    }
    mapped = orders.get(normalized)
    if mapped is not None:
        return mapped
    msg = f"Unsupported sort order: {value!r}"
    raise ValueError(msg)


def _string_tuple(value: object, *, label: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    msg = f"{label} must be a list of strings."
    raise TypeError(msg)


def _mapping_sequence(value: object, *, label: str) -> tuple[Mapping[str, object], ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        items: list[Mapping[str, object]] = []
        for item in value:
            if isinstance(item, Mapping):
                items.append(item)
            else:
                msg = f"{label} entries must be mappings."
                raise TypeError(msg)
        return tuple(items)
    msg = f"{label} must be a list of mappings."
    raise TypeError(msg)


def _parse_dedupe_strategy(
    value: object,
) -> Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]:
    if value is None:
        return "KEEP_FIRST_AFTER_SORT"
    normalized = str(value)
    allowed = {
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    }
    if normalized in allowed:
        return cast(
            "Literal['KEEP_FIRST_AFTER_SORT', 'KEEP_BEST_BY_SCORE', 'COLLAPSE_LIST', 'KEEP_ARBITRARY']",
            normalized,
        )
    msg = f"Unsupported dedupe strategy: {value!r}"
    raise ValueError(msg)


def _dedupe_from_row(
    payload: Mapping[str, object] | None,
) -> schema_system.DedupeSpecSpec | None:
    if payload is None:
        return None
    tie_breakers_payload = _mapping_sequence(payload.get("tie_breakers"), label="tie_breakers")
    tie_breakers = tuple(_sort_key_from_row(item) for item in tie_breakers_payload)
    return schema_system.DedupeSpecSpec(
        keys=_string_tuple(payload.get("keys"), label="keys"),
        tie_breakers=tie_breakers,
        strategy=_parse_dedupe_strategy(payload.get("strategy")),
    )


def _validation_from_row(payload: Mapping[str, object] | None) -> ArrowValidationOptions | None:
    if payload is None:
        return None
    strict_value = payload.get("strict")
    strict = _decode_strict("filter" if strict_value is None else str(strict_value))
    return ArrowValidationOptions(
        strict=strict,
        coerce=bool(payload.get("coerce", False)),
        max_errors=_parse_max_errors(payload.get("max_errors")),
        emit_invalid_rows=bool(payload.get("emit_invalid_rows", True)),
        emit_error_table=bool(payload.get("emit_error_table", True)),
    )


def _parse_max_errors(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    msg = "max_errors must be an int or None."
    raise TypeError(msg)


def contract_specs_from_table(
    table: pa.Table,
    table_specs: Mapping[str, TableSchemaSpec],
) -> dict[str, schema_system.ContractSpec]:
    """Compile ContractSpecs from a contract spec table.

    Returns
    -------
    dict[str, ContractSpec]
        Mapping of contract name to contract spec.

    Raises
    ------
    ValueError
        Raised when a contract references an unknown table spec.
    """
    specs: dict[str, schema_system.ContractSpec] = {}
    for row in table.to_pylist():
        name = str(row["name"])
        table_name = str(row["table_name"])
        table_spec = table_specs.get(table_name)
        if table_spec is None:
            msg = f"Unknown table spec for contract {name!r}: {table_name!r}."
            raise ValueError(msg)
        canonical = tuple(_sort_key_from_row(item) for item in row.get("canonical_sort") or ())
        virtual_docs = row.get("virtual_field_docs")
        specs[name] = schema_system.ContractSpec(
            name=name,
            table_schema=table_spec,
            dedupe=_dedupe_from_row(row.get("dedupe")),
            canonical_sort=canonical,
            version=row.get("version"),
            virtual_fields=tuple(row.get("virtual_fields") or ()),
            virtual_field_docs=dict(virtual_docs) if virtual_docs else None,
            validation=_validation_from_row(row.get("validation")),
        )
    return specs


def dataset_specs_from_tables(
    field_table: pa.Table,
    constraints_table: pa.Table | None = None,
    contract_table: pa.Table | None = None,
) -> dict[str, schema_system.DatasetSpec]:
    """Compile DatasetSpecs from spec tables.

    Returns
    -------
    dict[str, DatasetSpec]
        Mapping of dataset name to DatasetSpec.
    """
    table_specs = table_specs_from_tables(field_table, constraints_table=constraints_table)
    contracts: dict[str, schema_system.ContractSpec] = {}
    if contract_table is not None:
        contracts = contract_specs_from_table(contract_table, table_specs)
    contracts_by_table = {spec.table_schema.name: spec for spec in contracts.values()}
    return {
        name: schema_system.DatasetSpec(
            table_spec=spec,
            contract_spec=contracts_by_table.get(name),
        )
        for name, spec in table_specs.items()
    }


def schema_spec_tables_from_dataset_specs(
    specs: Iterable[schema_system.DatasetSpec],
) -> SchemaSpecTables:
    """Build schema spec tables from dataset specs.

    Returns
    -------
    SchemaSpecTables
        Bundle of spec tables derived from dataset specs.
    """
    spec_list = sorted(specs, key=lambda spec: spec.table_spec.name)
    table_specs = [spec.table_spec for spec in spec_list]
    field_table = field_spec_table(table_specs)
    constraints_table = table_constraints_table(table_specs)
    contract_specs = [spec.contract_spec for spec in spec_list if spec.contract_spec is not None]
    contract_table = contract_spec_table(contract_specs) if contract_specs else None
    return SchemaSpecTables(
        field_table=field_table,
        constraints_table=constraints_table,
        contract_table=contract_table,
    )


__all__ = [
    "CONTRACT_SPEC_SCHEMA",
    "DEDUPE_STRUCT",
    "FIELD_SPEC_SCHEMA",
    "SORT_KEY_STRUCT",
    "TABLE_CONSTRAINTS_SCHEMA",
    "VALIDATION_STRUCT",
    "SchemaSpecTables",
    "contract_spec_table",
    "contract_specs_from_table",
    "dataset_specs_from_tables",
    "field_spec_table",
    "schema_spec_tables_from_dataset_specs",
    "table_constraints_table",
    "table_specs_from_tables",
]
