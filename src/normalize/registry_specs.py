"""Registry accessors for normalize dataset specifications."""

from __future__ import annotations

from collections.abc import Iterable

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import SchemaMetadataSpec
from normalize.registry_builders import build_dataset_spec, build_input_schema
from normalize.registry_rows import DATASET_ROWS, DatasetRow
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ContractSpec, DatasetSpec

_DATASET_ROWS: dict[str, DatasetRow] = {row.name: row for row in DATASET_ROWS}
_DATASET_SPECS: dict[str, DatasetSpec] = {row.name: build_dataset_spec(row) for row in DATASET_ROWS}
_INPUT_SCHEMAS: dict[str, SchemaLike] = {row.name: build_input_schema(row) for row in DATASET_ROWS}


def dataset_row(name: str) -> DatasetRow:
    """Return the dataset row definition by name.

    Returns
    -------
    DatasetRow
        Dataset row definition.
    """
    return _DATASET_ROWS[name]


def dataset_rows() -> tuple[DatasetRow, ...]:
    """Return all dataset rows.

    Returns
    -------
    tuple[DatasetRow, ...]
        Dataset rows in registry order.
    """
    return DATASET_ROWS


def dataset_names() -> tuple[str, ...]:
    """Return dataset names in registry order.

    Returns
    -------
    tuple[str, ...]
        Dataset names.
    """
    return tuple(row.name for row in DATASET_ROWS)


def dataset_spec(name: str) -> DatasetSpec:
    """Return a DatasetSpec by name.

    Returns
    -------
    DatasetSpec
        Registered dataset spec.
    """
    return _DATASET_SPECS[name]


def dataset_specs() -> Iterable[DatasetSpec]:
    """Return all dataset specs.

    Returns
    -------
    Iterable[DatasetSpec]
        Dataset specifications.
    """
    return _DATASET_SPECS.values()


def dataset_table_spec(name: str) -> TableSchemaSpec:
    """Return the TableSchemaSpec for a dataset.

    Returns
    -------
    TableSchemaSpec
        Table schema specification.
    """
    return dataset_spec(name).table_spec


def dataset_contract(name: str) -> ContractSpec:
    """Return the ContractSpec for a dataset.

    Returns
    -------
    ContractSpec
        Contract specification for the dataset.

    Raises
    ------
    ValueError
        Raised when the dataset does not define a contract spec.
    """
    contract = dataset_spec(name).contract_spec
    if contract is None:
        msg = f"Dataset {name!r} does not define a contract spec."
        raise ValueError(msg)
    return contract


def dataset_contract_schema(name: str) -> SchemaLike:
    """Return the contract schema for a dataset.

    Returns
    -------
    SchemaLike
        Arrow schema defined by the dataset contract.
    """
    return dataset_contract(name).to_contract().schema


def dataset_query(name: str) -> QuerySpec:
    """Return the QuerySpec for a dataset.

    Returns
    -------
    QuerySpec
        Query specification for the dataset.
    """
    return dataset_spec(name).query()


def dataset_schema(name: str) -> SchemaLike:
    """Return the schema for a dataset with metadata applied.

    Returns
    -------
    SchemaLike
        Dataset schema with metadata.
    """
    return dataset_spec(name).schema()


def dataset_metadata_spec(name: str) -> SchemaMetadataSpec:
    """Return the metadata spec for a dataset.

    Returns
    -------
    SchemaMetadataSpec
        Metadata specification for the dataset.
    """
    return dataset_spec(name).metadata_spec


def dataset_input_schema(name: str) -> SchemaLike:
    """Return the input schema for a dataset.

    Returns
    -------
    SchemaLike
        Input schema for plan sources.
    """
    return _INPUT_SCHEMAS[name]


def dataset_input_columns(name: str) -> tuple[str, ...]:
    """Return the input column names for a dataset.

    Returns
    -------
    tuple[str, ...]
        Input column names.
    """
    return tuple(dataset_input_schema(name).names)


def dataset_schema_policy(name: str, *, ctx: ExecutionContext) -> SchemaPolicy:
    """Return a schema policy for a dataset spec.

    Returns
    -------
    SchemaPolicy
        Schema policy derived from the dataset table spec.
    """
    spec = dataset_spec(name)
    contract = spec.contract()
    options = SchemaPolicyOptions(
        schema=contract.with_versioned_schema(),
        encoding=spec.encoding_policy(),
        metadata=dataset_metadata_spec(name),
        validation=contract.validation,
    )
    return schema_policy_factory(spec.table_spec, ctx=ctx, options=options)


__all__ = [
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_columns",
    "dataset_input_schema",
    "dataset_metadata_spec",
    "dataset_names",
    "dataset_query",
    "dataset_row",
    "dataset_rows",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "dataset_specs",
    "dataset_table_spec",
]
