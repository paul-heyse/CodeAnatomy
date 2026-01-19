"""Shared helpers for dataset registry accessors."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike
    from schema_spec.system import ContractSpec, DatasetSpec


class NamedRow(Protocol):
    """Protocol for dataset row types with a name attribute."""

    @property
    def name(self) -> str:
        """Return the dataset row name.

        Returns
        -------
        str
            Dataset row name.
        """
        ...


@dataclass(frozen=True)
class DatasetRegistry[RowT: NamedRow]:
    """Immutable registry definition for dataset rows and builders."""

    rows: tuple[RowT, ...]
    build_dataset_spec: Callable[[RowT], DatasetSpec]
    _rows_by_name: Mapping[str, RowT] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize the rows-by-name lookup cache."""
        object.__setattr__(self, "_rows_by_name", {row.name: row for row in self.rows})

    @property
    def rows_by_name(self) -> Mapping[str, RowT]:
        """Return dataset rows keyed by name.

        Returns
        -------
        Mapping[str, RowT]
            Dataset rows keyed by name.
        """
        return self._rows_by_name


@dataclass
class DatasetAccessors[RowT: NamedRow]:
    """Cached accessors for dataset registry metadata."""

    registry: DatasetRegistry[RowT]
    _spec_cache: dict[str, DatasetSpec] = field(default_factory=dict, repr=False)
    _schema_cache: dict[str, SchemaLike] = field(default_factory=dict, repr=False)
    _contract_cache: dict[str, ContractSpec] = field(default_factory=dict, repr=False)

    def dataset_row(self, name: str) -> RowT:
        """Return the dataset row by name.

        Returns
        -------
        RowT
            Dataset row definition.
        """
        return self.registry.rows_by_name[name]

    def dataset_rows(self) -> tuple[RowT, ...]:
        """Return dataset rows in registry order.

        Returns
        -------
        tuple[RowT, ...]
            Dataset rows in registry order.
        """
        return self.registry.rows

    def dataset_names(self) -> tuple[str, ...]:
        """Return dataset names in registry order.

        Returns
        -------
        tuple[str, ...]
            Dataset names in registry order.
        """
        return tuple(row.name for row in self.registry.rows)

    def dataset_spec(self, name: str) -> DatasetSpec:
        """Return a cached DatasetSpec for a dataset name.

        Returns
        -------
        DatasetSpec
            Dataset specification for the name.
        """
        spec = self._spec_cache.get(name)
        if spec is None:
            spec = self.registry.build_dataset_spec(self.dataset_row(name))
            self._spec_cache[name] = spec
        return spec

    def dataset_specs(self) -> Iterable[DatasetSpec]:
        """Return all DatasetSpec instances in registry order.

        Yields
        ------
        DatasetSpec
            Dataset specification in registry order.
        """
        for row in self.registry.rows:
            yield self.dataset_spec(row.name)

    def dataset_schema(self, name: str) -> SchemaLike:
        """Return a cached schema for a dataset name.

        Returns
        -------
        SchemaLike
            Dataset schema for the name.
        """
        schema = self._schema_cache.get(name)
        if schema is None:
            schema = self.dataset_spec(name).schema()
            self._schema_cache[name] = schema
        return schema

    def dataset_contract_spec(self, name: str) -> ContractSpec:
        """Return a cached ContractSpec for a dataset name.

        Returns
        -------
        ContractSpec
            Contract specification for the name.
        """
        contract = self._contract_cache.get(name)
        if contract is None:
            contract = self.dataset_spec(name).contract_spec_or_default()
            self._contract_cache[name] = contract
        return contract


__all__ = ["DatasetAccessors", "DatasetRegistry", "NamedRow"]
