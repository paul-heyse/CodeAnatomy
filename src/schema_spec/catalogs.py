"""Schema contract catalogs."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator

from schema_spec.contracts import ContractSpec


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
