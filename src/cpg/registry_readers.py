"""Decode CPG registry tables into typed registry objects."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
from pyarrow import ipc

from arrowdsl.schema.build import iter_rows_from_table
from arrowdsl.spec.codec import (
    parse_dedupe_strategy,
    parse_mapping_sequence,
    parse_sort_order,
    parse_string_tuple,
)
from cpg.kinds_registry_enums import EdgeKind, NodeKind, SourceKind
from cpg.kinds_registry_models import (
    DerivationSpec,
    DerivationStatus,
    EdgeKindContract,
    NodeKindContract,
)
from cpg.kinds_registry_props import resolve_prop_specs
from cpg.registry_rows import DatasetRow
from cpg.registry_templates import RegistryTemplate
from registry_common.registry_rows import ContractRow
from schema_spec.specs import ArrowFieldSpec, FieldBundle
from schema_spec.system import DedupeSpecSpec, SortKeySpec, TableSpecConstraints

METADATA_PAIR_LEN = 2


@dataclass(frozen=True)
class RegistryCatalogs:
    """Decoded catalogs for CPG registry tables."""

    field_catalog: Mapping[str, ArrowFieldSpec]
    bundle_catalog: Mapping[str, FieldBundle]
    templates: Mapping[str, RegistryTemplate]


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


def _required_int(value: object | None, *, label: str) -> int:
    if isinstance(value, bool):
        msg = f"{label} must be an int."
        raise TypeError(msg)
    if isinstance(value, int):
        return value
    msg = f"{label} must be an int."
    raise TypeError(msg)


def _optional_int(value: object | None, *, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        msg = f"{label} must be an int or None."
        raise TypeError(msg)
    if isinstance(value, int):
        return value
    msg = f"{label} must be an int or None."
    raise TypeError(msg)


def _sort_key_from_row(payload: Mapping[str, object]) -> SortKeySpec:
    return SortKeySpec(
        column=str(payload.get("column")),
        order=parse_sort_order(payload.get("order")),
    )


def _dedupe_from_row(payload: Mapping[str, object] | None) -> DedupeSpecSpec | None:
    if payload is None:
        return None
    tie_breakers_payload = parse_mapping_sequence(
        payload.get("tie_breakers"),
        label="tie_breakers",
    )
    tie_breakers = tuple(_sort_key_from_row(item) for item in tie_breakers_payload)
    return DedupeSpecSpec(
        keys=parse_string_tuple(payload.get("keys"), label="keys"),
        tie_breakers=tie_breakers,
        strategy=parse_dedupe_strategy(payload.get("strategy")),
    )


def _constraints_from_row(payload: Mapping[str, object] | None) -> TableSpecConstraints | None:
    if payload is None:
        return None
    return TableSpecConstraints(
        required_non_null=parse_string_tuple(
            payload.get("required_non_null"),
            label="constraints.required_non_null",
        ),
        key_fields=parse_string_tuple(payload.get("key_fields"), label="constraints.key_fields"),
    )


def _contract_from_row(payload: Mapping[str, object] | None) -> ContractRow | None:
    if payload is None:
        return None
    canonical_payload = parse_mapping_sequence(
        payload.get("canonical_sort"),
        label="canonical_sort",
    )
    canonical = tuple(_sort_key_from_row(item) for item in canonical_payload)
    dedupe_payload = payload.get("dedupe")
    dedupe_mapping = dedupe_payload if isinstance(dedupe_payload, Mapping) else None
    return ContractRow(
        dedupe=_dedupe_from_row(dedupe_mapping),
        canonical_sort=canonical,
        constraints=parse_string_tuple(payload.get("constraints"), label="constraints"),
        version=_optional_int(payload.get("version"), label="contract.version"),
    )


def dataset_rows_from_table(table: pa.Table) -> tuple[DatasetRow, ...]:
    """Decode dataset rows from a registry table.

    Returns
    -------
    tuple[DatasetRow, ...]
        Dataset rows decoded from the table.
    """
    rows: list[DatasetRow] = []
    for record in iter_rows_from_table(table):
        constraints_payload = record.get("constraints")
        constraints_mapping = (
            constraints_payload if isinstance(constraints_payload, Mapping) else None
        )
        contract_payload = record.get("contract")
        contract_mapping = contract_payload if isinstance(contract_payload, Mapping) else None
        rows.append(
            DatasetRow(
                name=str(record.get("name")),
                version=_required_int(record.get("version"), label="version"),
                bundles=parse_string_tuple(record.get("bundles"), label="bundles"),
                fields=parse_string_tuple(record.get("fields"), label="fields"),
                constraints=_constraints_from_row(constraints_mapping),
                contract=_contract_from_row(contract_mapping),
                template=str(record.get("template")) if record.get("template") else None,
                contract_name=str(record.get("contract_name"))
                if record.get("contract_name")
                else None,
            )
        )
    return tuple(rows)


def _encode_field_metadata(value: object | None) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): str(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        meta: dict[str, str] = {}
        for item in value:
            if not isinstance(item, (list, tuple)) or len(item) != METADATA_PAIR_LEN:
                msg = "metadata must be a mapping."
                raise TypeError(msg)
            key, entry = item
            meta[str(key)] = str(entry)
        return meta
    msg = "metadata must be a mapping."
    raise TypeError(msg)


def _coerce_bytes(value: object) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    msg = "dtype must be bytes."
    raise TypeError(msg)


def _decode_dtype(payload: bytes) -> pa.DataType:
    reader = pa.BufferReader(payload)
    schema = ipc.read_schema(reader)
    return schema.field(0).type


def _coerce_encoding(value: object | None) -> Literal["dictionary"] | None:
    if value is None:
        return None
    normalized = str(value)
    if not normalized:
        return None
    if normalized == "dictionary":
        return "dictionary"
    msg = "encoding must be 'dictionary' or None."
    raise TypeError(msg)


def field_catalog_from_table(table: pa.Table) -> dict[str, ArrowFieldSpec]:
    """Decode field catalog entries from a registry table.

    Returns
    -------
    dict[str, ArrowFieldSpec]
        Field catalog mapping.
    """
    fields: dict[str, ArrowFieldSpec] = {}
    for record in iter_rows_from_table(table):
        name = str(record.get("name"))
        dtype = _decode_dtype(_coerce_bytes(record.get("dtype")))
        nullable = bool(record.get("nullable"))
        encoding = _coerce_encoding(record.get("encoding"))
        metadata = _encode_field_metadata(record.get("metadata"))
        fields[name] = ArrowFieldSpec(
            name=name,
            dtype=dtype,
            nullable=nullable,
            metadata=metadata,
            encoding=encoding,
        )
    return fields


def bundle_catalog_from_table(
    table: pa.Table,
    *,
    field_catalog: Mapping[str, ArrowFieldSpec],
) -> dict[str, FieldBundle]:
    """Decode bundle catalog entries from a registry table.

    Returns
    -------
    dict[str, FieldBundle]
        Bundle catalog mapping.
    """
    bundles: dict[str, FieldBundle] = {}
    for record in iter_rows_from_table(table):
        name = str(record.get("name"))
        field_names = parse_string_tuple(record.get("fields"), label="fields")
        fields = tuple(field_catalog[field] for field in field_names)
        bundles[name] = FieldBundle(
            name=name,
            fields=fields,
            required_non_null=parse_string_tuple(
                record.get("required_non_null"),
                label="required_non_null",
            ),
            key_fields=parse_string_tuple(record.get("key_fields"), label="key_fields"),
        )
    return bundles


def registry_templates_from_table(table: pa.Table) -> dict[str, RegistryTemplate]:
    """Decode registry templates from a registry table.

    Returns
    -------
    dict[str, RegistryTemplate]
        Registry template mapping.
    """
    templates: dict[str, RegistryTemplate] = {}
    for record in iter_rows_from_table(table):
        name = str(record.get("name"))
        templates[name] = RegistryTemplate(
            stage=str(record.get("stage")),
            determinism_tier=str(record.get("determinism_tier")),
        )
    return templates


def node_contracts_from_table(table: pa.Table) -> dict[NodeKind, NodeKindContract]:
    """Decode node contracts from a registry table.

    Returns
    -------
    dict[NodeKind, NodeKindContract]
        Node kind contracts keyed by kind.
    """
    contracts: dict[NodeKind, NodeKindContract] = {}
    for row in iter_rows_from_table(table):
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
    for row in iter_rows_from_table(table):
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
    for row in iter_rows_from_table(table):
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


__all__ = [
    "RegistryCatalogs",
    "bundle_catalog_from_table",
    "dataset_rows_from_table",
    "derivations_from_table",
    "edge_contracts_from_table",
    "field_catalog_from_table",
    "node_contracts_from_table",
    "registry_templates_from_table",
]
