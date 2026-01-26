"""Canonical relationship output schema and helpers."""

from __future__ import annotations

from functools import cache

from arrowdsl.core.interop import SchemaLike
from arrowdsl.core.ordering import OrderingLevel
from arrowdsl.schema.metadata import ordering_metadata_spec
from datafusion_engine.runtime import dataset_schema_from_context
from datafusion_engine.schema_contracts import SchemaContract, schema_contract_from_dataset_spec
from datafusion_engine.schema_registry import (
    REL_CALLSITE_QNAME_SCHEMA,
    REL_CALLSITE_SYMBOL_SCHEMA,
    REL_DEF_SYMBOL_SCHEMA,
    REL_IMPORT_SYMBOL_SCHEMA,
    REL_NAME_SYMBOL_SCHEMA,
)
from relspec.errors import RelspecValidationError
from schema_spec.system import (
    Contract,
    DatasetSpec,
    dataset_spec_from_schema,
    dataset_table_column_defaults,
    dataset_table_constraints,
    dataset_table_definition,
    dataset_table_logical_plan,
)

REL_NAME_SYMBOL_NAME = "rel_name_symbol_v1"
REL_IMPORT_SYMBOL_NAME = "rel_import_symbol_v1"
REL_DEF_SYMBOL_NAME = "rel_def_symbol_v1"
REL_CALLSITE_SYMBOL_NAME = "rel_callsite_symbol_v1"
REL_CALLSITE_QNAME_NAME = "rel_callsite_qname_v1"
RELATION_OUTPUT_NAME = "relation_output_v1"

RELATION_OUTPUT_ORDERING_KEYS: tuple[tuple[str, str], ...] = (
    ("path", "ascending"),
    ("bstart", "ascending"),
    ("bend", "ascending"),
    ("edge_owner_file_id", "ascending"),
    ("src", "ascending"),
    ("dst", "ascending"),
    ("task_priority", "ascending"),
    ("task_name", "ascending"),
)

RELATION_OUTPUT_SCHEMA = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=RELATION_OUTPUT_ORDERING_KEYS,
).apply(dataset_schema_from_context(RELATION_OUTPUT_NAME))


@cache
def rel_name_symbol_spec() -> DatasetSpec:
    """Return the DatasetSpec for name-symbol relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for name-symbol outputs.
    """
    return dataset_spec_from_schema(REL_NAME_SYMBOL_NAME, REL_NAME_SYMBOL_SCHEMA, version=1)


@cache
def rel_import_symbol_spec() -> DatasetSpec:
    """Return the DatasetSpec for import-symbol relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for import-symbol outputs.
    """
    return dataset_spec_from_schema(REL_IMPORT_SYMBOL_NAME, REL_IMPORT_SYMBOL_SCHEMA, version=1)


@cache
def rel_def_symbol_spec() -> DatasetSpec:
    """Return the DatasetSpec for def-symbol relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for def-symbol outputs.
    """
    return dataset_spec_from_schema(REL_DEF_SYMBOL_NAME, REL_DEF_SYMBOL_SCHEMA, version=1)


@cache
def rel_callsite_symbol_spec() -> DatasetSpec:
    """Return the DatasetSpec for callsite-symbol relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for callsite-symbol outputs.
    """
    return dataset_spec_from_schema(REL_CALLSITE_SYMBOL_NAME, REL_CALLSITE_SYMBOL_SCHEMA, version=1)


@cache
def rel_callsite_qname_spec() -> DatasetSpec:
    """Return the DatasetSpec for callsite-qname relationship outputs.

    Returns
    -------
    DatasetSpec
        Dataset specification for callsite-qname outputs.
    """
    return dataset_spec_from_schema(REL_CALLSITE_QNAME_NAME, REL_CALLSITE_QNAME_SCHEMA, version=1)


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


def rel_name_symbol_schema() -> SchemaLike:
    """Return the Arrow schema for name-symbol relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for name-symbol outputs.
    """
    return rel_name_symbol_spec().schema()


def rel_import_symbol_schema() -> SchemaLike:
    """Return the Arrow schema for import-symbol relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for import-symbol outputs.
    """
    return rel_import_symbol_spec().schema()


def rel_def_symbol_schema() -> SchemaLike:
    """Return the Arrow schema for def-symbol relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for def-symbol outputs.
    """
    return rel_def_symbol_spec().schema()


def rel_callsite_symbol_schema() -> SchemaLike:
    """Return the Arrow schema for callsite-symbol relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for callsite-symbol outputs.
    """
    return rel_callsite_symbol_spec().schema()


def rel_callsite_qname_schema() -> SchemaLike:
    """Return the Arrow schema for callsite-qname relationship outputs.

    Returns
    -------
    SchemaLike
        Arrow schema for callsite-qname outputs.
    """
    return rel_callsite_qname_spec().schema()


def relation_output_contract() -> Contract:
    """Return the Contract for canonical relationship outputs.

    Returns
    -------
    Contract
        Runtime contract for relationship outputs.
    """
    return relation_output_spec().contract()


@cache
def relation_output_schema_contract() -> SchemaContract:
    """Return the SchemaContract for canonical relationship outputs.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = relation_output_spec()
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


@cache
def rel_name_symbol_schema_contract() -> SchemaContract:
    """Return the SchemaContract for name-symbol outputs.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = rel_name_symbol_spec()
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


@cache
def rel_import_symbol_schema_contract() -> SchemaContract:
    """Return the SchemaContract for import-symbol outputs.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = rel_import_symbol_spec()
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


@cache
def rel_def_symbol_schema_contract() -> SchemaContract:
    """Return the SchemaContract for def-symbol outputs.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = rel_def_symbol_spec()
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


@cache
def rel_callsite_symbol_schema_contract() -> SchemaContract:
    """Return the SchemaContract for callsite-symbol outputs.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = rel_callsite_symbol_spec()
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


@cache
def rel_callsite_qname_schema_contract() -> SchemaContract:
    """Return the SchemaContract for callsite-qname outputs.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = rel_callsite_qname_spec()
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


def relation_output_ddl(*, dialect: str | None = None) -> str:
    """Return a CREATE TABLE statement for the relation output contract.

    Parameters
    ----------
    dialect
        Optional dialect hint (ignored; DataFusion is authoritative).

    Returns
    -------
    str
        CREATE TABLE statement for contract auditing.

    Raises
    ------
    RelspecValidationError
        Raised when DataFusion cannot provide a CREATE TABLE statement.
    """
    _ = dialect
    ddl = dataset_table_definition(RELATION_OUTPUT_NAME)
    if ddl is None:
        msg = "DataFusion did not return a CREATE TABLE statement for relation_output_v1."
        raise RelspecValidationError(msg)
    return ddl


def relation_output_constraints() -> tuple[str, ...]:
    """Return constraint metadata for the relation output contract.

    Returns
    -------
    tuple[str, ...]
        Constraint expressions when available.
    """
    return dataset_table_constraints(RELATION_OUTPUT_NAME)


def relation_output_column_defaults() -> dict[str, object]:
    """Return column default metadata for the relation output contract.

    Returns
    -------
    dict[str, object]
        Column default expressions when available.
    """
    return dataset_table_column_defaults(RELATION_OUTPUT_NAME)


def relation_output_logical_plan() -> str | None:
    """Return logical plan metadata for the relation output contract.

    Returns
    -------
    str | None
        Logical plan text when available.
    """
    return dataset_table_logical_plan(RELATION_OUTPUT_NAME)


__all__ = [
    "RELATION_OUTPUT_NAME",
    "RELATION_OUTPUT_SCHEMA",
    "REL_CALLSITE_QNAME_NAME",
    "REL_CALLSITE_SYMBOL_NAME",
    "REL_DEF_SYMBOL_NAME",
    "REL_IMPORT_SYMBOL_NAME",
    "REL_NAME_SYMBOL_NAME",
    "rel_callsite_qname_schema",
    "rel_callsite_qname_schema_contract",
    "rel_callsite_qname_spec",
    "rel_callsite_symbol_schema",
    "rel_callsite_symbol_schema_contract",
    "rel_callsite_symbol_spec",
    "rel_def_symbol_schema",
    "rel_def_symbol_schema_contract",
    "rel_def_symbol_spec",
    "rel_import_symbol_schema",
    "rel_import_symbol_schema_contract",
    "rel_import_symbol_spec",
    "rel_name_symbol_schema",
    "rel_name_symbol_schema_contract",
    "rel_name_symbol_spec",
    "relation_output_column_defaults",
    "relation_output_constraints",
    "relation_output_contract",
    "relation_output_ddl",
    "relation_output_logical_plan",
    "relation_output_schema",
    "relation_output_schema_contract",
    "relation_output_spec",
]
