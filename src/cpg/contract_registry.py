"""Contract and derivation registries backed by Arrow tables."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cache

import pyarrow as pa

from cpg.kinds_registry_enums import EdgeKind, NodeKind, SourceKind
from cpg.kinds_registry_models import (
    DerivationSpec,
    DerivationStatus,
    EdgeKindContract,
    NodeKindContract,
)
from cpg.kinds_registry_props import resolve_prop_specs
from cpg.registry_tables import (
    derivation_table as _derivation_table,
)
from cpg.registry_tables import (
    edge_contract_table as _edge_contract_table,
)
from cpg.registry_tables import (
    node_contract_table as _node_contract_table,
)


def _as_str_list(value: object | None) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _source_kinds(values: Sequence[str]) -> tuple[SourceKind, ...]:
    return tuple(SourceKind(value) for value in values)


def _derivation_status(value: object | None) -> DerivationStatus:
    normalized = str(value)
    if normalized == "planned":
        return "planned"
    return "implemented"


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


def node_contracts_from_table(table: pa.Table) -> dict[NodeKind, NodeKindContract]:
    """Decode node contracts from a registry table.

    Returns
    -------
    dict[NodeKind, NodeKindContract]
        Node kind contracts keyed by kind.
    """
    contracts: dict[NodeKind, NodeKindContract] = {}
    for row in table.to_pylist():
        kind = NodeKind(str(row["kind"]))
        required_keys = tuple(_as_str_list(row.get("required_props")))
        optional_keys = tuple(_as_str_list(row.get("optional_props")))
        sources = _source_kinds(_as_str_list(row.get("allowed_sources")))
        contracts[kind] = NodeKindContract(
            requires_anchor=bool(row["requires_anchor"]),
            required_props=resolve_prop_specs(required_keys),
            optional_props=resolve_prop_specs(optional_keys),
            allowed_sources=sources,
            description=str(row.get("description", "")),
        )
    return contracts


def edge_contracts_from_table(table: pa.Table) -> dict[EdgeKind, EdgeKindContract]:
    """Decode edge contracts from a registry table.

    Returns
    -------
    dict[EdgeKind, EdgeKindContract]
        Edge kind contracts keyed by kind.
    """
    contracts: dict[EdgeKind, EdgeKindContract] = {}
    for row in table.to_pylist():
        kind = EdgeKind(str(row["kind"]))
        required_keys = tuple(_as_str_list(row.get("required_props")))
        optional_keys = tuple(_as_str_list(row.get("optional_props")))
        sources = _source_kinds(_as_str_list(row.get("allowed_sources")))
        contracts[kind] = EdgeKindContract(
            requires_evidence_anchor=bool(row["requires_evidence_anchor"]),
            required_props=resolve_prop_specs(required_keys),
            optional_props=resolve_prop_specs(optional_keys),
            allowed_sources=sources,
            description=str(row.get("description", "")),
        )
    return contracts


def derivations_from_table(
    table: pa.Table,
) -> dict[NodeKind | EdgeKind, list[DerivationSpec]]:
    """Decode derivation specs from a registry table.

    Returns
    -------
    dict[NodeKind | EdgeKind, list[DerivationSpec]]
        Derivation specs keyed by kind.
    """
    out: dict[NodeKind | EdgeKind, list[DerivationSpec]] = {}
    for row in table.to_pylist():
        entity_kind = str(row.get("entity_kind", ""))
        kind_value = str(row.get("kind", ""))
        kind: NodeKind | EdgeKind = (
            NodeKind(kind_value) if entity_kind == "node" else EdgeKind(kind_value)
        )
        spec = DerivationSpec(
            extractor=str(row.get("extractor", "")),
            provider_or_field=str(row.get("provider_or_field", "")),
            join_keys=tuple(_as_str_list(row.get("join_keys"))),
            id_recipe=str(row.get("id_recipe", "")),
            confidence_policy=str(row.get("confidence_policy", "")),
            ambiguity_policy=str(row.get("ambiguity_policy", "")),
            status=_derivation_status(row.get("status")),
            notes=str(row.get("notes", "")),
        )
        out.setdefault(kind, []).append(spec)
    return out


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
