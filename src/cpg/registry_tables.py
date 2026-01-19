"""Arrow spec tables for the Ultimate CPG registry."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.schema.build import table_from_rows
from cpg.kinds_ultimate import (
    EDGE_DERIVATIONS,
    EDGE_KIND_CONTRACTS,
    NODE_DERIVATIONS,
    NODE_KIND_CONTRACTS,
)

NODE_CONTRACT_SCHEMA = pa.schema(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("requires_anchor", pa.bool_(), nullable=False),
        pa.field("required_props", pa.list_(pa.string()), nullable=False),
        pa.field("optional_props", pa.list_(pa.string()), nullable=False),
        pa.field("allowed_sources", pa.list_(pa.string()), nullable=False),
        pa.field("description", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"cpg_node_contracts"},
)

EDGE_CONTRACT_SCHEMA = pa.schema(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("requires_evidence_anchor", pa.bool_(), nullable=False),
        pa.field("required_props", pa.list_(pa.string()), nullable=False),
        pa.field("optional_props", pa.list_(pa.string()), nullable=False),
        pa.field("allowed_sources", pa.list_(pa.string()), nullable=False),
        pa.field("description", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"cpg_edge_contracts"},
)

DERIVATION_SCHEMA = pa.schema(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("entity_kind", pa.string(), nullable=False),
        pa.field("extractor", pa.string(), nullable=False),
        pa.field("provider_or_field", pa.string(), nullable=False),
        pa.field("join_keys", pa.list_(pa.string()), nullable=False),
        pa.field("id_recipe", pa.string(), nullable=False),
        pa.field("confidence_policy", pa.string(), nullable=False),
        pa.field("ambiguity_policy", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("notes", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"cpg_kind_derivations"},
)


def node_contract_table() -> pa.Table:
    """Build the node contract registry table.

    Returns
    -------
    pa.Table
        Arrow table of node contracts.
    """
    rows = [
        {
            "kind": kind.value,
            "requires_anchor": contract.requires_anchor,
            "required_props": list(contract.required_props.keys()),
            "optional_props": list(contract.optional_props.keys()),
            "allowed_sources": [src.value for src in contract.allowed_sources],
            "description": contract.description,
        }
        for kind, contract in NODE_KIND_CONTRACTS.items()
    ]
    return table_from_rows(NODE_CONTRACT_SCHEMA, rows)


def edge_contract_table() -> pa.Table:
    """Build the edge contract registry table.

    Returns
    -------
    pa.Table
        Arrow table of edge contracts.
    """
    rows = [
        {
            "kind": kind.value,
            "requires_evidence_anchor": contract.requires_evidence_anchor,
            "required_props": list(contract.required_props.keys()),
            "optional_props": list(contract.optional_props.keys()),
            "allowed_sources": [src.value for src in contract.allowed_sources],
            "description": contract.description,
        }
        for kind, contract in EDGE_KIND_CONTRACTS.items()
    ]
    return table_from_rows(EDGE_CONTRACT_SCHEMA, rows)


def derivation_table() -> pa.Table:
    """Build a combined derivation registry table.

    Returns
    -------
    pa.Table
        Arrow table of node/edge derivations.
    """
    rows: list[dict[str, object]] = []
    rows.extend(
        {
            "kind": kind.value,
            "entity_kind": "node",
            "extractor": spec.extractor,
            "provider_or_field": spec.provider_or_field,
            "join_keys": list(spec.join_keys),
            "id_recipe": spec.id_recipe,
            "confidence_policy": spec.confidence_policy,
            "ambiguity_policy": spec.ambiguity_policy,
            "status": spec.status,
            "notes": spec.notes,
        }
        for kind, derivations in NODE_DERIVATIONS.items()
        for spec in derivations
    )
    rows.extend(
        {
            "kind": kind.value,
            "entity_kind": "edge",
            "extractor": spec.extractor,
            "provider_or_field": spec.provider_or_field,
            "join_keys": list(spec.join_keys),
            "id_recipe": spec.id_recipe,
            "confidence_policy": spec.confidence_policy,
            "ambiguity_policy": spec.ambiguity_policy,
            "status": spec.status,
            "notes": spec.notes,
        }
        for kind, derivations in EDGE_DERIVATIONS.items()
        for spec in derivations
    )
    return table_from_rows(DERIVATION_SCHEMA, rows)


__all__ = [
    "DERIVATION_SCHEMA",
    "EDGE_CONTRACT_SCHEMA",
    "NODE_CONTRACT_SCHEMA",
    "derivation_table",
    "edge_contract_table",
    "node_contract_table",
]
