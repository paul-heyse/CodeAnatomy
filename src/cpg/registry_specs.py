"""Programmatic registry accessors for CPG datasets."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

from cpg.registry_builders import build_dataset_spec
from cpg.registry_readers import dataset_rows_from_table
from cpg.registry_rows import DatasetRow
from cpg.registry_tables import dataset_rows_table
from registry_common.dataset_registry import DatasetAccessors, DatasetRegistry

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike
    from schema_spec.system import ContractSpec, DatasetSpec


@cache
def _dataset_rows() -> tuple[DatasetRow, ...]:
    return dataset_rows_from_table(dataset_rows_table())


_REGISTRY = DatasetRegistry(rows=_dataset_rows(), build_dataset_spec=build_dataset_spec)
_ACCESSORS = DatasetAccessors(_REGISTRY)


def dataset_row(name: str) -> DatasetRow:
    """Return the dataset row spec by name.

    Returns
    -------
    DatasetRow
        Row specification for the dataset.
    """
    return _ACCESSORS.dataset_row(name)


def dataset_spec(name: str) -> DatasetSpec:
    """Return the DatasetSpec for the dataset name.

    Returns
    -------
    DatasetSpec
        Dataset specification for the name.
    """
    return _ACCESSORS.dataset_spec(name)


def dataset_schema(name: str) -> SchemaLike:
    """Return the Arrow schema for the dataset name.

    Returns
    -------
    SchemaLike
        Arrow schema for the dataset.
    """
    return _ACCESSORS.dataset_schema(name)


def dataset_contract_spec(name: str) -> ContractSpec:
    """Return the ContractSpec for the dataset name.

    Returns
    -------
    ContractSpec
        Contract specification for the dataset.
    """
    return _ACCESSORS.dataset_contract_spec(name)


__all__ = [
    "dataset_contract_spec",
    "dataset_row",
    "dataset_schema",
    "dataset_spec",
]
