"""Registry accessors for incremental dataset specifications."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.schema_authority import (
    dataset_schema_from_context,
    dataset_spec_from_context,
)
from incremental.registry_rows import DATASET_ROWS, DatasetRow

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike
    from schema_spec.system import ContractSpec, DatasetSpec

_DATASET_ROWS: dict[str, DatasetRow] = {row.name: row for row in DATASET_ROWS}


def dataset_row(name: str) -> DatasetRow:
    """Return the dataset row spec by name.

    Returns
    -------
    DatasetRow
        Row specification for the dataset.
    """
    return _DATASET_ROWS[name]


def dataset_spec(name: str) -> DatasetSpec:
    """Return the DatasetSpec for the dataset name.

    Returns
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    return dataset_spec_from_context(name)


def dataset_schema(name: str) -> SchemaLike:
    """Return the Arrow schema for the dataset name.

    Returns
    -------
    SchemaLike
        Arrow schema for the dataset.
    """
    return dataset_schema_from_context(name)


def dataset_contract_spec(name: str) -> ContractSpec:
    """Return the ContractSpec for the dataset name.

    Returns
    -------
    ContractSpec
        Contract specification for the dataset.
    """
    return dataset_spec_from_context(name).contract_spec_or_default()


__all__ = [
    "dataset_contract_spec",
    "dataset_row",
    "dataset_schema",
    "dataset_spec",
]
