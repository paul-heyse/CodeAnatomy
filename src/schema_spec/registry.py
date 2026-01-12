"""Central registry for table and contract specifications."""

from __future__ import annotations

from dataclasses import dataclass, field

from schema_spec.contracts import ContractSpec
from schema_spec.core import TableSchemaSpec


@dataclass(frozen=True)
class SchemaRegistry:
    """Registry for schema and contract specs."""

    table_specs: dict[str, TableSchemaSpec] = field(default_factory=dict)
    contract_specs: dict[str, ContractSpec] = field(default_factory=dict)

    def register_table(self, spec: TableSchemaSpec) -> TableSchemaSpec:
        """Register or return an existing table spec.

        Returns
        -------
        TableSchemaSpec
            Registered table spec.
        """
        existing = self.table_specs.get(spec.name)
        if existing is not None:
            return existing
        self.table_specs[spec.name] = spec
        return spec

    def register_contract(self, spec: ContractSpec) -> ContractSpec:
        """Register or return an existing contract spec.

        Returns
        -------
        ContractSpec
            Registered contract spec.
        """
        existing = self.contract_specs.get(spec.name)
        if existing is not None:
            return existing
        self.contract_specs[spec.name] = spec
        return spec
