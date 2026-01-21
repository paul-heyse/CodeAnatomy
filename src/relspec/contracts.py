"""Canonical relationship output schema and helpers."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.schema.metadata import merge_metadata_specs, ordering_metadata_spec
from arrowdsl.schema.schema import SchemaMetadataSpec
from schema_spec.system import Contract, DatasetSpec, dataset_spec_from_schema

RELATION_OUTPUT_NAME = "relation_output_v1"

RELATION_OUTPUT_ORDERING_KEYS: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("bstart", "ascending"),
    ("bend", "ascending"),
    ("edge_owner_file_id", "ascending"),
    ("src", "ascending"),
    ("dst", "ascending"),
    ("rule_priority", "ascending"),
    ("rule_name", "ascending"),
)

_RELATION_OUTPUT_BASE_SCHEMA = pa.schema(
    [
        pa.field("src", pa.string(), nullable=True),
        pa.field("dst", pa.string(), nullable=True),
        pa.field("path", pa.string(), nullable=True),
        pa.field("edge_owner_file_id", pa.string(), nullable=True),
        pa.field("bstart", pa.int64(), nullable=True),
        pa.field("bend", pa.int64(), nullable=True),
        pa.field("origin", pa.string(), nullable=True),
        pa.field("resolution_method", pa.string(), nullable=True),
        pa.field("confidence", pa.float32(), nullable=True),
        pa.field("score", pa.float32(), nullable=True),
        pa.field("symbol_roles", pa.int32(), nullable=True),
        pa.field("qname_source", pa.string(), nullable=True),
        pa.field("ambiguity_group_id", pa.string(), nullable=True),
        pa.field("diag_source", pa.string(), nullable=True),
        pa.field("severity", pa.string(), nullable=True),
        pa.field("rule_name", pa.string(), nullable=True),
        pa.field("rule_priority", pa.int32(), nullable=True),
    ],
    metadata={b"spec_kind": b"relation_output"},
)
RELATION_OUTPUT_SCHEMA = merge_metadata_specs(
    SchemaMetadataSpec(schema_metadata=_RELATION_OUTPUT_BASE_SCHEMA.metadata or {}),
    ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=RELATION_OUTPUT_ORDERING_KEYS),
).apply(_RELATION_OUTPUT_BASE_SCHEMA)


@cache
def relation_output_spec() -> DatasetSpec:
    """Return the DatasetSpec for canonical relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for relationship outputs.
    """
    return dataset_spec_from_schema(RELATION_OUTPUT_NAME, RELATION_OUTPUT_SCHEMA, version=1)


def relation_output_schema() -> SchemaLike:
    """Return the Arrow schema for canonical relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for relationship outputs.
    """
    return relation_output_spec().schema()


def relation_output_contract() -> Contract:
    """Return the Contract for canonical relationship outputs.

    Returns
    -------
    Contract
        Runtime contract for relationship outputs.
    """
    return relation_output_spec().contract()


def relation_output_ddl(*, dialect: str | None = None) -> str:
    """Return a CREATE TABLE statement for the relation output contract.

    Returns
    -------
    str
        CREATE TABLE statement for contract auditing.
    """
    spec = relation_output_spec()
    return spec.table_spec.to_create_table_sql(dialect=dialect)


__all__ = [
    "RELATION_OUTPUT_NAME",
    "RELATION_OUTPUT_SCHEMA",
    "relation_output_contract",
    "relation_output_ddl",
    "relation_output_schema",
    "relation_output_spec",
]
