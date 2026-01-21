"""Canonical relationship output schema and helpers."""

from __future__ import annotations

from functools import cache

from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.schema_authority import dataset_schema_from_context
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

RELATION_OUTPUT_SCHEMA = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=RELATION_OUTPUT_ORDERING_KEYS,
).apply(dataset_schema_from_context(RELATION_OUTPUT_NAME))


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
