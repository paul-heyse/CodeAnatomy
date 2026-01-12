"""Helpers for registering extractor datasets."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.core.context import OrderingKey, OrderingLevel
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec
from schema_spec.specs import ArrowFieldSpec, FieldBundle
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    ContractSpec,
    DatasetSpec,
    make_dataset_spec,
    make_table_spec,
)


@dataclass(frozen=True)
class DatasetRegistration:
    """Optional registration settings for extract datasets."""

    query_spec: QuerySpec | None = None
    contract_spec: ContractSpec | None = None
    evolution_spec: SchemaEvolutionSpec | None = None
    metadata_spec: SchemaMetadataSpec | None = None


def register_dataset(
    *,
    name: str,
    version: int,
    fields: Sequence[ArrowFieldSpec],
    bundles: Sequence[FieldBundle] = (),
    registration: DatasetRegistration | None = None,
) -> DatasetSpec:
    """Register a dataset spec with the global schema registry.

    Returns
    -------
    DatasetSpec
        Registered dataset specification.
    """
    registration = registration or DatasetRegistration()
    return GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name=name,
                version=version,
                bundles=tuple(bundles),
                fields=list(fields),
            ),
            query_spec=registration.query_spec,
            contract_spec=registration.contract_spec,
            evolution_spec=registration.evolution_spec,
            metadata_spec=registration.metadata_spec,
        )
    )


def ordering_metadata_spec(
    level: OrderingLevel,
    *,
    keys: Sequence[OrderingKey] = (),
    extra: dict[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata describing ordering semantics.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec with ordering annotations.
    """
    meta = {b"ordering_level": level.value.encode("utf-8")}
    if keys:
        key_text = ",".join(f"{col}:{order}" for col, order in keys)
        meta[b"ordering_keys"] = key_text.encode("utf-8")
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def extractor_metadata_spec(
    name: str,
    version: int,
    *,
    extra: dict[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata for extractor provenance.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec with extractor provenance fields.
    """
    meta = {
        b"extractor_name": name.encode("utf-8"),
        b"extractor_version": str(version).encode("utf-8"),
    }
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


__all__ = [
    "DatasetRegistration",
    "extractor_metadata_spec",
    "ordering_metadata_spec",
    "register_dataset",
]
