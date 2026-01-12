"""Factory helpers for table specs, contracts, and query specs."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from arrowdsl.pyarrow_protocols import SchemaLike
from arrowdsl.queryspec import ProjectionSpec, QuerySpec
from schema_spec.contracts import ContractSpec, DedupeSpecSpec, SortKeySpec
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec
from schema_spec.fields import FieldBundle


@dataclass(frozen=True)
class TableSpecConstraints:
    """Required and key fields for a table schema."""

    required_non_null: Iterable[str] = ()
    key_fields: Iterable[str] = ()


@dataclass(frozen=True)
class VirtualFieldSpec:
    """Virtual field names and documentation for a contract."""

    fields: tuple[str, ...] = ()
    docs: Mapping[str, str] | None = None


def _merge_names(*parts: Iterable[str]) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for name in part:
            if name in seen:
                continue
            merged.append(name)
            seen.add(name)
    return tuple(merged)


def _merge_fields(
    bundles: Iterable[FieldBundle],
    fields: Iterable[ArrowFieldSpec],
) -> list[ArrowFieldSpec]:
    merged: list[ArrowFieldSpec] = []
    for bundle in bundles:
        merged.extend(bundle.fields)
    merged.extend(fields)
    return merged


def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[ArrowFieldSpec],
    constraints: TableSpecConstraints | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from field bundles and explicit fields.

    Parameters
    ----------
    name:
        Table name.
    version:
        Optional schema version.
    bundles:
        Field bundles to merge.
    fields:
        Explicit fields to append.
    constraints:
        Optional required and key field constraints.

    Returns
    -------
    TableSchemaSpec
        Table schema specification.
    """
    if constraints is None:
        constraints = TableSpecConstraints()
    bundle_required = (bundle.required_non_null for bundle in bundles)
    bundle_keys = (bundle.key_fields for bundle in bundles)
    return TableSchemaSpec(
        name=name,
        version=version,
        fields=_merge_fields(bundles, fields),
        required_non_null=_merge_names(*bundle_required, constraints.required_non_null),
        key_fields=_merge_names(*bundle_keys, constraints.key_fields),
    )


def make_contract_spec(
    *,
    table_spec: TableSchemaSpec,
    dedupe: DedupeSpecSpec | None = None,
    canonical_sort: Iterable[SortKeySpec] = (),
    virtual: VirtualFieldSpec | None = None,
    version: int | None = None,
) -> ContractSpec:
    """Create a ContractSpec from a TableSchemaSpec.

    Returns
    -------
    ContractSpec
        Contract specification.
    """
    virtual_fields = virtual.fields if virtual is not None else ()
    virtual_docs = dict(virtual.docs) if virtual is not None and virtual.docs is not None else None
    return ContractSpec(
        name=table_spec.name,
        table_schema=table_spec,
        dedupe=dedupe,
        canonical_sort=tuple(canonical_sort),
        version=version if version is not None else table_spec.version,
        virtual_fields=virtual_fields,
        virtual_field_docs=virtual_docs,
    )


def query_spec_for_table(table_spec: TableSchemaSpec) -> QuerySpec:
    """Create a QuerySpec projecting the table spec columns.

    Returns
    -------
    QuerySpec
        Query spec for the table columns.
    """
    cols = tuple(field.name for field in table_spec.fields)
    return QuerySpec(projection=ProjectionSpec(base=cols))


def _decode_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from a pyarrow.Schema.

    Parameters
    ----------
    name:
        Table spec name.
    schema:
        Arrow schema instance.
    version:
        Optional schema version override.

    Returns
    -------
    TableSchemaSpec
        Table schema specification.
    """
    fields = [
        ArrowFieldSpec(
            name=field.name,
            dtype=field.type,
            nullable=field.nullable,
            metadata=_decode_metadata(field.metadata),
        )
        for field in schema
    ]
    return TableSchemaSpec(name=name, version=version, fields=fields)
