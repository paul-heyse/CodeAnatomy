"""Registry accessors for incremental dataset specifications."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.session.runtime import dataset_schema_from_context, dataset_spec_from_context

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import SchemaLike
    from schema_spec.system import ContractSpec, DatasetSpec


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


__all__ = ["dataset_contract_spec", "dataset_schema", "dataset_spec"]
