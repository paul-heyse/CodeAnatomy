"""Contract catalogs for relationship rules."""

from __future__ import annotations

from datafusion_engine.finalize import Contract
from schema_spec.system import ContractCatalogSpec


class ContractCatalog:
    """Map contract names to ``Contract`` objects."""

    def __init__(self) -> None:
        self._contracts: dict[str, Contract] = {}

    def register(self, contract: Contract) -> None:
        """Register a contract.

        Parameters
        ----------
        contract
            Contract to register.
        """
        self._contracts[contract.name] = contract

    @classmethod
    def from_spec(cls, spec: ContractCatalogSpec) -> ContractCatalog:
        """Build a catalog from a validated contract spec.

        Parameters
        ----------
        spec
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

    def get(self, name: str) -> Contract:
        """Return a registered contract by name.

        Parameters
        ----------
        name
            Contract name to resolve.

        Returns
        -------
        Contract
            The requested contract.

        Raises
        ------
        KeyError
            Raised when the contract name is not registered.
        """
        contract = self._contracts.get(name)
        if contract is None:
            msg = f"ContractCatalog: unknown contract {name!r}."
            raise KeyError(msg)
        return contract

    def has(self, name: str) -> bool:
        """Return whether a contract name is registered.

        Parameters
        ----------
        name
            Contract name to check.

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


__all__ = ["ContractCatalog"]
