"""Arrow spec tables for the Ultimate CPG registry."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.interop import DataTypeLike
from arrowdsl.schema.build import list_view_type, table_from_rows
from arrowdsl.spec.infra import DEDUPE_STRUCT, SORT_KEY_STRUCT
from cpg.kinds_ultimate import (
    EDGE_DERIVATIONS,
    EDGE_KIND_CONTRACTS,
    NODE_DERIVATIONS,
    NODE_KIND_CONTRACTS,
)
from cpg.registry_bundles import bundle_catalog
from cpg.registry_fields import field_catalog
from cpg.registry_rows import DATASET_ROWS, DatasetRow
from cpg.registry_templates import registry_templates
from schema_spec.system import DedupeSpecSpec, SortKeySpec, TableSpecConstraints

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

CPG_CONSTRAINTS_STRUCT = pa.struct(
    [
        pa.field("required_non_null", list_view_type(pa.string()), nullable=True),
        pa.field("key_fields", list_view_type(pa.string()), nullable=True),
    ]
)

CPG_CONTRACT_ROW_STRUCT = pa.struct(
    [
        pa.field("dedupe", DEDUPE_STRUCT, nullable=True),
        pa.field("canonical_sort", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("constraints", pa.list_(pa.string()), nullable=True),
        pa.field("version", pa.int64(), nullable=True),
    ]
)

CPG_DATASET_ROW_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("version", pa.int32(), nullable=False),
        pa.field("bundles", list_view_type(pa.string()), nullable=True),
        pa.field("fields", list_view_type(pa.string()), nullable=True),
        pa.field("constraints", CPG_CONSTRAINTS_STRUCT, nullable=True),
        pa.field("contract", CPG_CONTRACT_ROW_STRUCT, nullable=True),
        pa.field("template", pa.string(), nullable=True),
        pa.field("contract_name", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"cpg_dataset_rows"},
)

CPG_FIELD_CATALOG_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("dtype", pa.binary(), nullable=False),
        pa.field("nullable", pa.bool_(), nullable=False),
        pa.field("encoding", pa.string(), nullable=True),
        pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"cpg_field_catalog"},
)

CPG_BUNDLE_CATALOG_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("fields", list_view_type(pa.string()), nullable=False),
        pa.field("required_non_null", list_view_type(pa.string()), nullable=True),
        pa.field("key_fields", list_view_type(pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"cpg_bundle_catalog"},
)

CPG_REGISTRY_TEMPLATES_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("stage", pa.string(), nullable=False),
        pa.field("determinism_tier", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"cpg_registry_templates"},
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


def _constraints_row(
    constraints: TableSpecConstraints | None,
) -> dict[str, object] | None:
    if constraints is None:
        return None
    required = list(constraints.required_non_null)
    key_fields = list(constraints.key_fields)
    return {
        "required_non_null": required or None,
        "key_fields": key_fields or None,
    }


def _sort_key_row(spec: SortKeySpec) -> dict[str, object]:
    return {"column": spec.column, "order": spec.order}


def _dedupe_row(dedupe: DedupeSpecSpec | None) -> dict[str, object] | None:
    if dedupe is None:
        return None
    return {
        "keys": list(dedupe.keys),
        "tie_breakers": [_sort_key_row(spec) for spec in dedupe.tie_breakers] or None,
        "strategy": dedupe.strategy,
    }


def _contract_row(row: DatasetRow) -> dict[str, object] | None:
    contract = row.contract
    if contract is None:
        return None
    return {
        "dedupe": _dedupe_row(contract.dedupe),
        "canonical_sort": [_sort_key_row(spec) for spec in contract.canonical_sort] or None,
        "constraints": list(contract.constraints) or None,
        "version": contract.version,
    }


def dataset_rows_table() -> pa.Table:
    """Build the dataset row registry table.

    Returns
    -------
    pa.Table
        Arrow table of dataset rows.
    """
    rows = [
        {
            "name": row.name,
            "version": row.version,
            "bundles": list(row.bundles) or None,
            "fields": list(row.fields) or None,
            "constraints": _constraints_row(row.constraints),
            "contract": _contract_row(row),
            "template": row.template,
            "contract_name": row.contract_name,
        }
        for row in DATASET_ROWS
    ]
    return table_from_rows(CPG_DATASET_ROW_SCHEMA, rows)


def _encode_dtype(dtype: DataTypeLike) -> bytes:
    schema = pa.schema([pa.field("field", dtype)])
    return schema.serialize().to_pybytes()


def field_catalog_table() -> pa.Table:
    """Build the field catalog registry table.

    Returns
    -------
    pa.Table
        Arrow table of field catalog entries.
    """
    rows = [
        {
            "name": name,
            "dtype": _encode_dtype(spec.dtype),
            "nullable": spec.nullable,
            "encoding": spec.encoding,
            "metadata": dict(spec.metadata) if spec.metadata else None,
        }
        for name, spec in field_catalog().items()
    ]
    return table_from_rows(CPG_FIELD_CATALOG_SCHEMA, rows)


def bundle_catalog_table() -> pa.Table:
    """Build the bundle catalog registry table.

    Returns
    -------
    pa.Table
        Arrow table of bundle catalog entries.
    """
    rows = [
        {
            "name": bundle.name,
            "fields": [field.name for field in bundle.fields],
            "required_non_null": list(bundle.required_non_null) or None,
            "key_fields": list(bundle.key_fields) or None,
        }
        for bundle in bundle_catalog().values()
    ]
    return table_from_rows(CPG_BUNDLE_CATALOG_SCHEMA, rows)


def registry_templates_table() -> pa.Table:
    """Build the registry template catalog table.

    Returns
    -------
    pa.Table
        Arrow table of registry template defaults.
    """
    rows = [
        {
            "name": name,
            "stage": template.stage,
            "determinism_tier": template.determinism_tier,
        }
        for name, template in registry_templates().items()
    ]
    return table_from_rows(CPG_REGISTRY_TEMPLATES_SCHEMA, rows)


__all__ = [
    "CPG_BUNDLE_CATALOG_SCHEMA",
    "CPG_CONSTRAINTS_STRUCT",
    "CPG_CONTRACT_ROW_STRUCT",
    "CPG_DATASET_ROW_SCHEMA",
    "CPG_FIELD_CATALOG_SCHEMA",
    "CPG_REGISTRY_TEMPLATES_SCHEMA",
    "DERIVATION_SCHEMA",
    "EDGE_CONTRACT_SCHEMA",
    "NODE_CONTRACT_SCHEMA",
    "bundle_catalog_table",
    "dataset_rows_table",
    "derivation_table",
    "edge_contract_table",
    "field_catalog_table",
    "node_contract_table",
    "registry_templates_table",
]
