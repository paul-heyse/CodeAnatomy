"""Schema contract catalogs."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator

from schema_spec.contracts import ContractSpec
from schema_spec.registry import SchemaRegistry


class ContractCatalogSpec(BaseModel):
    """Collection of contract specifications keyed by name."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    contracts: dict[str, ContractSpec]

    @field_validator("contracts")
    @classmethod
    def _names_match(cls, contracts: dict[str, ContractSpec]) -> dict[str, ContractSpec]:
        mismatched = [name for name, spec in contracts.items() if name != spec.name]
        if mismatched:
            msg = f"contract key mismatch: {mismatched}"
            raise ValueError(msg)
        return contracts

    def to_contracts(self) -> dict[str, ContractSpec]:
        """Return a name->spec mapping.

        Returns
        -------
        dict[str, ContractSpec]
            Copy of the contracts mapping.
        """
        return dict(self.contracts)

    def register_into(self, registry: SchemaRegistry) -> SchemaRegistry:
        """Register catalog entries into a schema registry.

        Parameters
        ----------
        registry:
            Registry to receive table and contract specs.

        Returns
        -------
        SchemaRegistry
            Registry with catalog specs registered.
        """
        for contract in self.contracts.values():
            registry.register_table(contract.table_schema)
            registry.register_contract(contract)
        return registry
