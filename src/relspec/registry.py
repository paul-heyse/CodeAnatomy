"""Catalogs and registries for datasets and contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.fs as pafs

from arrowdsl.finalize.finalize import Contract
from arrowdsl.spec.io import read_spec_table
from arrowdsl.spec.tables.schema import contract_specs_from_table, table_specs_from_tables
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ContractCatalogSpec, DatasetSpec

type PathLike = str | Path


@dataclass(frozen=True)
class DatasetLocation:
    """Location metadata for a dataset."""

    path: PathLike
    format: str = "parquet"
    partitioning: str | None = "hive"
    filesystem: pafs.FileSystem | None = None
    table_spec: TableSchemaSpec | None = None
    dataset_spec: DatasetSpec | None = None


class DatasetCatalog:
    """Map dataset names to locations for plan resolution."""

    def __init__(self) -> None:
        self._locs: dict[str, DatasetLocation] = {}

    def register(self, name: str, location: DatasetLocation) -> None:
        """Register a dataset location.

        Parameters
        ----------
        name:
            Dataset name.
        location:
            Location metadata.

        Raises
        ------
        ValueError
            Raised when the dataset name is empty.
        """
        if not name:
            msg = "DatasetCatalog.register: name must be non-empty."
            raise ValueError(msg)
        self._locs[name] = location

    def get(self, name: str) -> DatasetLocation:
        """Return a registered dataset location.

        Parameters
        ----------
        name:
            Dataset name.

        Returns
        -------
        DatasetLocation
            Location metadata for the dataset.

        Raises
        ------
        KeyError
            Raised when the dataset name is not registered.
        """
        if name not in self._locs:
            msg = f"DatasetCatalog: unknown dataset {name!r}."
            raise KeyError(msg)
        return self._locs[name]

    def has(self, name: str) -> bool:
        """Return whether a dataset name is registered.

        Parameters
        ----------
        name:
            Dataset name.

        Returns
        -------
        bool
            ``True`` when the dataset is registered.
        """
        return name in self._locs

    def names(self) -> list[str]:
        """Return registered dataset names in sorted order.

        Returns
        -------
        list[str]
            Sorted dataset names.
        """
        return sorted(self._locs)


class ContractCatalog:
    """Map contract names to ``Contract`` objects."""

    def __init__(self) -> None:
        self._contracts: dict[str, Contract] = {}

    def register(self, contract: Contract) -> None:
        """Register a contract.

        Parameters
        ----------
        contract:
            Contract to register.
        """
        self._contracts[contract.name] = contract

    @classmethod
    def from_spec(cls, spec: ContractCatalogSpec) -> ContractCatalog:
        """Build a catalog from a validated contract spec.

        Parameters
        ----------
        spec:
            Contract catalog spec with validated names.

        Returns
        -------
        ContractCatalog
            Catalog populated with spec-derived contracts.
        """
        catalog = cls()
        for contract_spec in spec.contracts.values():
            catalog.register(contract_spec.to_contract())
        return catalog

    @classmethod
    def from_tables(
        cls,
        *,
        field_table: pa.Table,
        constraints_table: pa.Table | None = None,
        contract_table: pa.Table | None = None,
    ) -> ContractCatalog:
        """Build a catalog from schema spec tables.

        Returns
        -------
        ContractCatalog
            Catalog populated from spec tables.
        """
        table_specs = table_specs_from_tables(field_table, constraints_table=constraints_table)
        contracts = (
            contract_specs_from_table(contract_table, table_specs)
            if contract_table is not None
            else {}
        )
        return cls.from_spec(ContractCatalogSpec(contracts=contracts))

    @classmethod
    def from_paths(
        cls,
        *,
        field_table: PathLike,
        constraints_table: PathLike | None = None,
        contract_table: PathLike | None = None,
    ) -> ContractCatalog:
        """Build a catalog from on-disk schema spec tables.

        Returns
        -------
        ContractCatalog
            Catalog populated from spec tables.
        """
        field = read_spec_table(field_table)
        constraints = read_spec_table(constraints_table) if constraints_table is not None else None
        contract = read_spec_table(contract_table) if contract_table is not None else None
        return cls.from_tables(
            field_table=field,
            constraints_table=constraints,
            contract_table=contract,
        )

    def get(self, name: str) -> Contract:
        """Return a registered contract by name.

        Parameters
        ----------
        name:
            Contract name.

        Returns
        -------
        Contract
            The requested contract.

        Raises
        ------
        KeyError
            Raised when the contract name is not registered.
        """
        if name not in self._contracts:
            msg = f"ContractCatalog: unknown contract {name!r}."
            raise KeyError(msg)
        return self._contracts[name]

    def has(self, name: str) -> bool:
        """Return whether a contract name is registered.

        Parameters
        ----------
        name:
            Contract name.

        Returns
        -------
        bool
            ``True`` when the contract is registered.
        """
        return name in self._contracts

    def names(self) -> list[str]:
        """Return registered contract names in sorted order.

        Returns
        -------
        list[str]
            Sorted contract names.
        """
        return sorted(self._contracts)
