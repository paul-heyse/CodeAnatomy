"""Registry accessors for incremental dataset specifications."""

from __future__ import annotations

from functools import cache

from arrowdsl.core.interop import SchemaLike
from incremental.registry_builders import build_dataset_spec
from incremental.registry_rows import DATASET_ROWS, DatasetRow
from schema_spec.system import ContractSpec, DatasetSpec

_ROWS_BY_NAME: dict[str, DatasetRow] = {row.name: row for row in DATASET_ROWS}


def dataset_row(name: str) -> DatasetRow:
    """Return the dataset row spec by name.

    Returns
    -------
    DatasetRow
        Row specification for the dataset.
    """
    return _ROWS_BY_NAME[name]


@cache
def dataset_spec(name: str) -> DatasetSpec:
    """Return the DatasetSpec for the dataset name.

    Returns
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    row = dataset_row(name)
    return build_dataset_spec(row)


@cache
def dataset_schema(name: str) -> SchemaLike:
    """Return the Arrow schema for the dataset name.

    Returns
    -------
    SchemaLike
        Arrow schema for the dataset.
    """
    return dataset_spec(name).schema()


@cache
def dataset_contract_spec(name: str) -> ContractSpec:
    """Return the ContractSpec for the dataset name.

    Returns
    -------
    ContractSpec
        Contract specification for the dataset.
    """
    return dataset_spec(name).contract_spec_or_default()


__all__ = [
    "dataset_contract_spec",
    "dataset_row",
    "dataset_schema",
    "dataset_spec",
]
