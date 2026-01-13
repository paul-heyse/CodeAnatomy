"""Factory helpers for registering schema specs and datasets."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TypedDict, Unpack

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.schema import SchemaEvolutionSpec, SchemaMetadataSpec
from arrowdsl.schema.validation import ArrowValidationOptions
from schema_spec.specs import ArrowFieldSpec, DerivedFieldSpec, FieldBundle, TableSchemaSpec
from schema_spec.system import (
    ContractSpec,
    DatasetSpec,
    SchemaRegistry,
    make_dataset_spec,
    make_table_spec,
    register_dataset_spec,
)


@dataclass(frozen=True)
class DatasetRegistration:
    """Optional registration settings for dataset specs."""

    query_spec: QuerySpec | None = None
    contract_spec: ContractSpec | None = None
    derived_fields: Sequence[DerivedFieldSpec] = ()
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
    evolution_spec: SchemaEvolutionSpec | None = None
    metadata_spec: SchemaMetadataSpec | None = None
    validation: ArrowValidationOptions | None = None


class TableSpecInputKwargs(TypedDict, total=False):
    """Keyword arguments supported by register_dataset table construction."""

    name: str
    version: int | None
    fields: Sequence[ArrowFieldSpec]
    bundles: Sequence[FieldBundle]


def register_dataset(
    *,
    table_spec: TableSchemaSpec | None = None,
    registration: DatasetRegistration | None = None,
    registry: SchemaRegistry | None = None,
    **table_kwargs: Unpack[TableSpecInputKwargs],
) -> DatasetSpec:
    """Register a dataset spec with the global schema registry.

    Returns
    -------
    DatasetSpec
        Registered dataset specification.

    Raises
    ------
    ValueError
        Raised when table_spec or name/fields are missing.
    """
    registration = registration or DatasetRegistration()
    if table_spec is None:
        name = table_kwargs.get("name")
        fields = table_kwargs.get("fields")
        if name is None or fields is None:
            msg = "register_dataset requires name/fields or an explicit table_spec."
            raise ValueError(msg)
        bundles = table_kwargs.get("bundles", ())
        version = table_kwargs.get("version")
        table_spec = make_table_spec(
            name=name,
            version=version,
            bundles=tuple(bundles),
            fields=list(fields),
        )
    spec = make_dataset_spec(
        table_spec=table_spec,
        query_spec=registration.query_spec,
        contract_spec=registration.contract_spec,
        derived_fields=registration.derived_fields,
        predicate=registration.predicate,
        pushdown_predicate=registration.pushdown_predicate,
        evolution_spec=registration.evolution_spec,
        metadata_spec=registration.metadata_spec,
        validation=registration.validation,
    )
    return register_dataset_spec(spec, registry=registry)


__all__ = ["DatasetRegistration", "register_dataset"]
