"""Catalogs and registries for datasets, contracts, and rules."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.fs as pafs

from arrowdsl.finalize.finalize import Contract
from arrowdsl.spec.io import read_spec_table
from arrowdsl.spec.tables.relspec import relationship_rules_from_table
from arrowdsl.spec.tables.schema import contract_specs_from_table, table_specs_from_tables
from relspec.model import RelationshipRule
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
        constraints = (
            read_spec_table(constraints_table) if constraints_table is not None else None
        )
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


class RelationshipRegistry:
    """Hold relationship rules and provide grouping utilities."""

    def __init__(self) -> None:
        self._rules_by_name: dict[str, RelationshipRule] = {}

    @classmethod
    def from_table(cls, table: pa.Table) -> RelationshipRegistry:
        """Build a registry from a relationship rule spec table.

        Returns
        -------
        RelationshipRegistry
            Registry populated from the table.
        """
        registry = cls()
        registry.extend(relationship_rules_from_table(table))
        return registry

    @classmethod
    def from_path(cls, path: PathLike) -> RelationshipRegistry:
        """Build a registry from an on-disk relationship rule spec table.

        Returns
        -------
        RelationshipRegistry
            Registry populated from the table.
        """
        return cls.from_table(read_spec_table(path))

    def add(self, rule: RelationshipRule) -> None:
        """Add a validated relationship rule.

        Parameters
        ----------
        rule:
            Relationship rule to add.

        Raises
        ------
        ValueError
            Raised when the rule name is duplicated.
        """
        if rule.name in self._rules_by_name:
            msg = f"Duplicate rule name: {rule.name!r}."
            raise ValueError(msg)
        self._rules_by_name[rule.name] = rule

    def extend(self, rules: Iterable[RelationshipRule]) -> None:
        """Add multiple relationship rules.

        Parameters
        ----------
        rules:
            Iterable of rules to add.
        """
        for rule in rules:
            self.add(rule)

    def get(self, name: str) -> RelationshipRule:
        """Return a rule by name.

        Parameters
        ----------
        name:
            Rule name.

        Returns
        -------
        RelationshipRule
            The requested rule.
        """
        return self._rules_by_name[name]

    def rules(self) -> list[RelationshipRule]:
        """Return all rules in sorted order.

        Returns
        -------
        list[RelationshipRule]
            Rules sorted by name.
        """
        return [self._rules_by_name[name] for name in sorted(self._rules_by_name)]

    def by_output(self) -> dict[str, list[RelationshipRule]]:
        """Group rules by output dataset with deterministic ordering.

        Returns
        -------
        dict[str, list[RelationshipRule]]
            Mapping of output dataset to rules.
        """
        out: dict[str, list[RelationshipRule]] = {}
        for rule in self._rules_by_name.values():
            out.setdefault(rule.output_dataset, []).append(rule)
        for key in list(out.keys()):
            out[key] = sorted(out[key], key=lambda rr: (rr.priority, rr.name))
        return out

    def outputs(self) -> list[str]:
        """Return output dataset names referenced by rules.

        Returns
        -------
        list[str]
            Sorted output dataset names.
        """
        return sorted({rule.output_dataset for rule in self._rules_by_name.values()})

    def inputs(self) -> list[str]:
        """Return input dataset names referenced by rules.

        Returns
        -------
        list[str]
            Sorted input dataset names.
        """
        names: set[str] = set()
        for rule in self._rules_by_name.values():
            for dref in rule.inputs:
                names.add(dref.name)
        return sorted(names)

    def validate_contract_consistency(self) -> None:
        """Enforce consistent contract names for shared outputs.

        Raises
        ------
        ValueError
            Raised when contract names differ across rules for an output dataset.
        """
        for out_name, rules in self.by_output().items():
            contracts = {rule.contract_name for rule in rules}
            if len(contracts) > 1:
                msg = (
                    "Output "
                    f"{out_name!r} has inconsistent contract_name across rules: "
                    f"{sorted(map(str, contracts))}."
                )
                raise ValueError(msg)
