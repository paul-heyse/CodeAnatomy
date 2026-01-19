"""Shared helpers for building dataset registry specs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from registry_common.registry_rows import ContractRow
from schema_spec.system import make_contract_spec

if TYPE_CHECKING:
    from schema_spec.system import ContractSpec, TableSchemaSpec


def build_contract_spec(
    contract: ContractRow | None,
    *,
    table_spec: TableSchemaSpec,
) -> ContractSpec | None:
    """Build a ContractSpec for a registry row.

    Returns
    -------
    ContractSpec | None
        Contract specification or ``None`` when no contract is provided.
    """
    if contract is None:
        return None
    return make_contract_spec(
        table_spec=table_spec,
        dedupe=contract.dedupe,
        canonical_sort=contract.canonical_sort,
        constraints=contract.constraints,
        version=contract.version,
    )


__all__ = ["build_contract_spec"]
