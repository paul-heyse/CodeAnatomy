"""Central dataset registry for DataFusion catalog registration."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from cpg.registry_builders import build_dataset_spec as build_cpg_dataset_spec
from cpg.registry_rows import DATASET_ROWS as CPG_DATASET_ROWS
from datafusion_engine.schema_registry import is_nested_dataset, nested_schema_for, schema_for
from extract.registry_builders import build_dataset_spec as build_extract_dataset_spec
from extract.registry_rows import DATASET_ROWS as EXTRACT_DATASET_ROWS
from incremental.registry_builders import build_dataset_spec as build_incremental_dataset_spec
from incremental.registry_rows import DATASET_ROWS as INCREMENTAL_DATASET_ROWS
from normalize.registry_builders import build_dataset_spec as build_normalize_dataset_spec
from normalize.registry_rows import DATASET_ROWS as NORMALIZE_DATASET_ROWS
from registry_common.dataset_registry import DatasetAccessors, DatasetRegistry, NamedRow
from relspec.contracts import relation_output_spec
from schema_spec.normalize_derived_specs import normalize_derived_specs
from schema_spec.relationship_specs import relationship_dataset_specs
from schema_spec.system import ContractSpec, DatasetSpec, SchemaRegistry

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike


def _build_registry[RowT: NamedRow](
    rows: Sequence[RowT],
    *,
    build_dataset_spec: Callable[[RowT], DatasetSpec],
) -> DatasetAccessors[RowT]:
    """Create cached accessors from dataset rows and spec builder.

    Parameters
    ----------
    rows
        Dataset registry rows used to build specs.
    build_dataset_spec
        Callable that converts a registry row into a dataset spec.

    Returns
    -------
    DatasetAccessors[RowT]
        Dataset accessors for the provided rows.
    """
    registry = DatasetRegistry(rows=tuple(rows), build_dataset_spec=build_dataset_spec)
    return DatasetAccessors(registry)


@dataclass(frozen=True)
class DatasetSpecCatalog:
    """Central catalog for dataset specs across domains."""

    accessors: tuple[DatasetAccessors[NamedRow], ...]
    extra_specs: tuple[DatasetSpec, ...] = ()
    _specs_by_name: Mapping[str, DatasetSpec] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Build the dataset spec lookup table.

        Raises
        ------
        ValueError
            Raised when duplicate dataset spec names are detected.
        """
        specs: dict[str, DatasetSpec] = {}
        for accessor in self.accessors:
            for spec in accessor.dataset_specs():
                if spec.name in specs:
                    msg = f"Duplicate dataset spec name: {spec.name!r}."
                    raise ValueError(msg)
                specs[spec.name] = spec
        for spec in self.extra_specs:
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
        if is_nested_dataset(name):
            return nested_schema_for(name, allow_derived=True)
        try:
            return schema_for(name)
        except KeyError:
            return self.dataset_spec(name).schema()

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
    accessors = (
        _build_registry(EXTRACT_DATASET_ROWS, build_dataset_spec=build_extract_dataset_spec),
        _build_registry(NORMALIZE_DATASET_ROWS, build_dataset_spec=build_normalize_dataset_spec),
        _build_registry(CPG_DATASET_ROWS, build_dataset_spec=build_cpg_dataset_spec),
        _build_registry(
            INCREMENTAL_DATASET_ROWS, build_dataset_spec=build_incremental_dataset_spec
        ),
    )
    extra_specs = (
        *normalize_derived_specs(),
        *relationship_dataset_specs(),
        relation_output_spec(),
    )
    typed_accessors = cast("tuple[DatasetAccessors[NamedRow], ...]", accessors)
    return DatasetSpecCatalog(accessors=typed_accessors, extra_specs=extra_specs)


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
    return dataset_spec_catalog().dataset_spec(name)


def dataset_specs() -> tuple[DatasetSpec, ...]:
    """Return all dataset specs from the central catalog.

    Returns
    -------
    tuple[DatasetSpec, ...]
        Dataset specs sorted by name.
    """
    return dataset_spec_catalog().dataset_specs()


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
    return dataset_spec_catalog().dataset_schema(name)


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
    return dataset_spec_catalog().dataset_contract_spec(name)


def dataset_names() -> tuple[str, ...]:
    """Return dataset names from the central catalog.

    Returns
    -------
    tuple[str, ...]
        Dataset names sorted by name.
    """
    return dataset_spec_catalog().dataset_names()


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
