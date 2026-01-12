"""Helpers for registering extractor datasets."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.metadata import (
    extractor_metadata_spec,
    infer_ordering_keys,
    merge_metadata_specs,
    options_hash,
    options_metadata_spec,
    ordering_metadata_spec,
)
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


__all__ = [
    "DatasetRegistration",
    "extractor_metadata_spec",
    "infer_ordering_keys",
    "merge_metadata_specs",
    "options_hash",
    "options_metadata_spec",
    "ordering_metadata_spec",
    "register_dataset",
]
