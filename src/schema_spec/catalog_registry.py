"""Central dataset registry for DataFusion catalog registration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from functools import cache
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_authority import (
    dataset_schema_from_context,
    dataset_spec_from_context,
)
from datafusion_engine.schema_introspection import SchemaIntrospector
from schema_spec.system import ContractSpec, DatasetSpec, SchemaRegistry

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike


@dataclass(frozen=True)
class DatasetSpecCatalog:
    """Central catalog for dataset specs across domains."""

    specs: tuple[DatasetSpec, ...]
    _specs_by_name: Mapping[str, DatasetSpec] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Build the dataset spec lookup table.

        Raises
        ------
        ValueError
            Raised when duplicate dataset spec names are detected.
        """
        specs: dict[str, DatasetSpec] = {}
        for spec in self.specs:
            if spec.name in specs:
                msg = f"Duplicate dataset spec name: {spec.name!r}."
                raise ValueError(msg)
            specs[spec.name] = spec
        object.__setattr__(self, "_specs_by_name", dict(specs))

    def dataset_spec(self, name: str) -> DatasetSpec:
        """Return the dataset spec for the provided name.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns
        -------
        DatasetSpec
            Dataset specification for the name.

        Raises
        ------
        KeyError
            Raised when the dataset name is unknown.
        """
        spec = self._specs_by_name.get(name)
        if spec is None:
            msg = f"Unknown dataset spec: {name!r}."
            raise KeyError(msg)
        return spec

    def dataset_specs(self) -> tuple[DatasetSpec, ...]:
        """Return dataset specs in stable order.

        Returns
        -------
        tuple[DatasetSpec, ...]
            Dataset specifications sorted by name.
        """
        return tuple(sorted(self._specs_by_name.values(), key=lambda spec: spec.name))

    def dataset_names(self) -> tuple[str, ...]:
        """Return dataset names in stable order.

        Returns
        -------
        tuple[str, ...]
            Dataset names sorted by name.
        """
        return tuple(sorted(self._specs_by_name))

    def dataset_contract_spec(self, name: str) -> ContractSpec:
        """Return the contract spec for the dataset.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns
        -------
        ContractSpec
            Contract specification for the dataset.
        """
        return self.dataset_spec(name).contract_spec_or_default()

    def dataset_schema(self, name: str) -> SchemaLike:
        """Return the schema for a dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns
        -------
        SchemaLike
            Dataset schema resolved from nested or canonical sources.
        """
        return dataset_schema_from_context(name)

    def dataset_schema_pyarrow(self, name: str) -> pa.Schema:
        """Return a pyarrow schema for a dataset name.

        Parameters
        ----------
        name
            Dataset name to resolve.

        Returns
        -------
        pyarrow.Schema
            Schema resolved as a pyarrow schema instance.

        Raises
        ------
        TypeError
            Raised when the resolved schema cannot be converted to pyarrow.
        """
        schema = self.dataset_schema(name)
        if isinstance(schema, pa.Schema):
            return schema
        to_pyarrow = getattr(schema, "to_pyarrow", None)
        if callable(to_pyarrow):
            resolved = to_pyarrow()
            if isinstance(resolved, pa.Schema):
                return resolved
        msg = f"Dataset schema for {name!r} did not resolve to pyarrow.Schema."
        raise TypeError(msg)


@cache
def dataset_spec_catalog() -> DatasetSpecCatalog:
    """Return the cached dataset spec catalog.

    Returns
    -------
    DatasetSpecCatalog
        Cached catalog of dataset specs.
    """
    return DatasetSpecCatalog(specs=dataset_specs())


def _dataset_names_from_context() -> tuple[str, ...]:
    ctx = DataFusionRuntimeProfile().session_context()
    introspector = SchemaIntrospector(ctx)
    names: set[str] = set()
    for row in introspector.tables_snapshot():
        table_name = row.get("table_name")
        table_schema = row.get("table_schema")
        if table_schema == "information_schema":
            continue
        if isinstance(table_name, str):
            names.add(table_name)
    return tuple(sorted(names))


def _default_contract_spec(spec: DatasetSpec) -> ContractSpec:
    return ContractSpec(name=spec.name, table_schema=spec.table_spec)


def _dataset_spec_with_contract(name: str) -> DatasetSpec:
    spec = dataset_spec_from_context(name)
    if spec.contract_spec is not None:
        return spec
    return replace(spec, contract_spec=_default_contract_spec(spec))


def dataset_spec(name: str) -> DatasetSpec:
    """Return a dataset spec from the central catalog.

    Parameters
    ----------
    name
        Dataset name to resolve.

    Returns
    -------
    DatasetSpec
        Dataset spec for the provided name.
    """
    return _dataset_spec_with_contract(name)


def dataset_specs() -> tuple[DatasetSpec, ...]:
    """Return all dataset specs from the central catalog.

    Returns
    -------
    tuple[DatasetSpec, ...]
        Dataset specs sorted by name.
    """
    return tuple(_dataset_spec_with_contract(name) for name in dataset_names())


def dataset_schema(name: str) -> SchemaLike:
    """Return a dataset schema from the central catalog.

    Parameters
    ----------
    name
        Dataset name to resolve.

    Returns
    -------
    SchemaLike
        Resolved dataset schema.
    """
    return dataset_schema_from_context(name)


def dataset_contract_spec(name: str) -> ContractSpec:
    """Return a dataset contract spec from the central catalog.

    Parameters
    ----------
    name
        Dataset name to resolve.

    Returns
    -------
    ContractSpec
        Contract spec for the dataset.
    """
    return dataset_spec(name).contract_spec or _default_contract_spec(dataset_spec(name))


def dataset_names() -> tuple[str, ...]:
    """Return dataset names from the central catalog.

    Returns
    -------
    tuple[str, ...]
        Dataset names sorted by name.
    """
    return _dataset_names_from_context()


def schema_registry() -> SchemaRegistry:
    """Return a SchemaRegistry populated from the central catalog.

    Returns
    -------
    SchemaRegistry
        Registry populated with dataset specs.
    """
    registry = SchemaRegistry()
    for spec in dataset_specs():
        registry.register_dataset(spec)
    return registry


__all__ = [
    "DatasetSpecCatalog",
    "dataset_contract_spec",
    "dataset_names",
    "dataset_schema",
    "dataset_spec",
    "dataset_spec_catalog",
    "dataset_specs",
    "schema_registry",
]
