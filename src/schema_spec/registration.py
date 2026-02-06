"""Dataset registration helpers for schema specs."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TypedDict, Unpack

from datafusion_engine.arrow.metadata import SchemaMetadataSpec
from datafusion_engine.delta.protocol import DeltaFeatureGate
from datafusion_engine.expr.query_spec import QuerySpec
from datafusion_engine.expr.spec import ExprSpec
from datafusion_engine.schema.alignment import SchemaEvolutionSpec
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import DerivedFieldSpec, FieldBundle, TableSchemaSpec
from schema_spec.system import (
    ContractSpec,
    DatasetKind,
    DatasetSpec,
    DeltaCdfPolicy,
    DeltaMaintenancePolicy,
    ValidationPolicySpec,
    make_dataset_spec,
    make_table_spec,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy


@dataclass(frozen=True)
class DatasetRegistration:
    """Optional registration settings for dataset specs."""

    dataset_kind: DatasetKind | None = None
    query_spec: QuerySpec | None = None
    contract_spec: ContractSpec | None = None
    delta_cdf_policy: DeltaCdfPolicy | None = None
    delta_maintenance_policy: DeltaMaintenancePolicy | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_feature_gate: DeltaFeatureGate | None = None
    delta_constraints: Sequence[str] = ()
    derived_fields: Sequence[DerivedFieldSpec] = ()
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
    evolution_spec: SchemaEvolutionSpec | None = None
    metadata_spec: SchemaMetadataSpec | None = None
    validation: ArrowValidationOptions | None = None
    dataframe_validation: ValidationPolicySpec | None = None


class TableSpecInputKwargs(TypedDict, total=False):
    """Keyword arguments supported by register_dataset table construction."""

    name: str
    version: int | None
    fields: Sequence[FieldSpec]
    bundles: Sequence[FieldBundle]


def register_dataset(
    *,
    table_spec: TableSchemaSpec | None = None,
    registration: DatasetRegistration | None = None,
    **table_kwargs: Unpack[TableSpecInputKwargs],
) -> DatasetSpec:
    """Build a dataset spec from the provided inputs.

    Args:
        table_spec: Optional table schema spec.
        registration: Optional dataset registration metadata.
        **table_kwargs: Table spec construction keyword args.

    Returns:
        DatasetSpec: Result.

    Raises:
        ValueError: If required dataset registration inputs are missing.
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
    return make_dataset_spec(
        table_spec=table_spec,
        dataset_kind=registration.dataset_kind or "primary",
        query_spec=registration.query_spec,
        contract_spec=registration.contract_spec,
        delta_cdf_policy=registration.delta_cdf_policy,
        delta_maintenance_policy=registration.delta_maintenance_policy,
        delta_write_policy=registration.delta_write_policy,
        delta_schema_policy=registration.delta_schema_policy,
        delta_feature_gate=registration.delta_feature_gate,
        delta_constraints=registration.delta_constraints,
        derived_fields=registration.derived_fields,
        predicate=registration.predicate,
        pushdown_predicate=registration.pushdown_predicate,
        evolution_spec=registration.evolution_spec,
        metadata_spec=registration.metadata_spec,
        validation=registration.validation,
        dataframe_validation=registration.dataframe_validation,
    )


__all__ = ["DatasetRegistration", "register_dataset"]
