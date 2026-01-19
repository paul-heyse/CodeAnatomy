"""Contract and derivation registries backed by Arrow tables."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from cpg.kinds_registry_enums import EdgeKind, NodeKind
from cpg.kinds_registry_models import DerivationSpec, EdgeKindContract, NodeKindContract
from cpg.registry_readers import (
    derivations_from_table,
    edge_contracts_from_table,
    node_contracts_from_table,
)
from cpg.registry_tables import (
    derivation_table as _derivation_table,
)
from cpg.registry_tables import (
    edge_contract_table as _edge_contract_table,
)
from cpg.registry_tables import (
    node_contract_table as _node_contract_table,
)


@cache
def node_contract_table() -> pa.Table:
    """Return the node contract registry table.

    Returns
    -------
    pa.Table
        Node contract table.
    """
    return _node_contract_table()


@cache
def edge_contract_table() -> pa.Table:
    """Return the edge contract registry table.

    Returns
    -------
    pa.Table
        Edge contract table.
    """
    return _edge_contract_table()


@cache
def derivation_table() -> pa.Table:
    """Return the derivation registry table.

    Returns
    -------
    pa.Table
        Derivation registry table.
    """
    return _derivation_table()


@cache
def node_kind_contracts() -> dict[NodeKind, NodeKindContract]:
    """Return node contracts decoded from the registry table.

    Returns
    -------
    dict[NodeKind, NodeKindContract]
        Node kind contracts keyed by kind.
    """
    return node_contracts_from_table(node_contract_table())


@cache
def edge_kind_contracts() -> dict[EdgeKind, EdgeKindContract]:
    """Return edge contracts decoded from the registry table.

    Returns
    -------
    dict[EdgeKind, EdgeKindContract]
        Edge kind contracts keyed by kind.
    """
    return edge_contracts_from_table(edge_contract_table())


@cache
def derivation_specs() -> dict[NodeKind | EdgeKind, list[DerivationSpec]]:
    """Return derivation specs decoded from the registry table.

    Returns
    -------
    dict[NodeKind | EdgeKind, list[DerivationSpec]]
        Derivation specs keyed by kind.
    """
    return derivations_from_table(derivation_table())


__all__ = [
    "derivation_specs",
    "derivation_table",
    "edge_contract_table",
    "edge_kind_contracts",
    "node_contract_table",
    "node_kind_contracts",
]
