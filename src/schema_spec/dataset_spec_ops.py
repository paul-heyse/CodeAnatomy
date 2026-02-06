"""Derived helpers for DatasetSpec to keep specs data-only."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import msgspec

from arrow_utils.core.ordering import Ordering, OrderingLevel
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import SchemaLike, TableLike
from datafusion_engine.arrow.metadata import (
    encoding_policy_from_spec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.expr.query_spec import ProjectionSpec, QuerySpec
from datafusion_engine.schema.alignment import CastErrorPolicy
from datafusion_engine.schema.finalize import Contract, FinalizeContext
from datafusion_engine.schema.policy import SchemaPolicyOptions, schema_policy_factory
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.system import (
    ContractSpec,
    DataFusionScanOptions,
    DatasetSpec,
    DeltaCdfPolicy,
    DeltaFeatureGate,
    DeltaMaintenancePolicy,
    DeltaPolicyBundle,
    DeltaScanOptions,
    DeltaSchemaPolicy,
    DeltaWritePolicy,
    ValidationPolicySpec,
    ViewSpec,
    _ordering_metadata_spec,
)

if TYPE_CHECKING:
    from schema_spec.dataset_handle import DatasetHandle


def dataset_spec_name(spec: DatasetSpec) -> str:
    """Return the dataset name.

    Returns:
    -------
    str
        Dataset name from the table spec.
    """
    return spec.table_spec.name


def dataset_spec_datafusion_scan(spec: DatasetSpec) -> DataFusionScanOptions | None:
    """Return DataFusion scan options from policy bundle.

    Returns:
    -------
    DataFusionScanOptions | None
        DataFusion scan options when configured.
    """
    return spec.policies.datafusion_scan


def dataset_spec_delta_scan(spec: DatasetSpec) -> DeltaScanOptions | None:
    """Return Delta scan options from policy bundle.

    Returns:
    -------
    DeltaScanOptions | None
        Delta scan options when configured.
    """
    if spec.policies.delta is None:
        return None
    return spec.policies.delta.scan


def dataset_spec_delta_cdf_policy(spec: DatasetSpec) -> DeltaCdfPolicy | None:
    """Return Delta CDF policy from policy bundle.

    Returns:
    -------
    DeltaCdfPolicy | None
        Delta CDF policy when configured.
    """
    if spec.policies.delta is None:
        return None
    return spec.policies.delta.cdf_policy


def dataset_spec_delta_maintenance_policy(spec: DatasetSpec) -> DeltaMaintenancePolicy | None:
    """Return Delta maintenance policy from policy bundle.

    Returns:
    -------
    DeltaMaintenancePolicy | None
        Delta maintenance policy when configured.
    """
    if spec.policies.delta is None:
        return None
    return spec.policies.delta.maintenance_policy


def dataset_spec_with_delta_maintenance(
    spec: DatasetSpec,
    maintenance_policy: DeltaMaintenancePolicy | None,
) -> DatasetSpec:
    """Return a DatasetSpec with updated Delta maintenance policy.

    Returns:
    -------
    DatasetSpec
        Dataset spec with updated maintenance policy.
    """
    if maintenance_policy is None:
        return spec
    delta = spec.policies.delta
    if delta is None:
        delta = DeltaPolicyBundle(maintenance_policy=maintenance_policy)
    else:
        delta = msgspec.structs.replace(delta, maintenance_policy=maintenance_policy)
    policies = msgspec.structs.replace(spec.policies, delta=delta)
    return msgspec.structs.replace(spec, policies=policies)


def dataset_spec_delta_write_policy(spec: DatasetSpec) -> DeltaWritePolicy | None:
    """Return Delta write policy from policy bundle.

    Returns:
    -------
    DeltaWritePolicy | None
        Delta write policy when configured.
    """
    if spec.policies.delta is None:
        return None
    return spec.policies.delta.write_policy


def dataset_spec_delta_schema_policy(spec: DatasetSpec) -> DeltaSchemaPolicy | None:
    """Return Delta schema policy from policy bundle.

    Returns:
    -------
    DeltaSchemaPolicy | None
        Delta schema policy when configured.
    """
    if spec.policies.delta is None:
        return None
    return spec.policies.delta.schema_policy


def dataset_spec_delta_feature_gate(spec: DatasetSpec) -> DeltaFeatureGate | None:
    """Return Delta feature gate from policy bundle.

    Returns:
    -------
    DeltaFeatureGate | None
        Delta feature gate when configured.
    """
    if spec.policies.delta is None:
        return None
    return spec.policies.delta.feature_gate


def dataset_spec_delta_constraints(spec: DatasetSpec) -> tuple[str, ...]:
    """Return Delta constraints from policy bundle.

    Returns:
    -------
    tuple[str, ...]
        Declared Delta constraints, if any.
    """
    if spec.policies.delta is None:
        return ()
    return spec.policies.delta.constraints


def dataset_spec_validation(spec: DatasetSpec) -> ArrowValidationOptions | None:
    """Return Arrow validation options from policy bundle.

    Returns:
    -------
    ArrowValidationOptions | None
        Arrow validation options when configured.
    """
    return spec.policies.validation


def dataset_spec_dataframe_validation(spec: DatasetSpec) -> ValidationPolicySpec | None:
    """Return DataFrame validation policy from policy bundle.

    Returns:
    -------
    ValidationPolicySpec | None
        DataFrame validation policy when configured.
    """
    return spec.policies.dataframe_validation


def dataset_spec_schema(spec: DatasetSpec) -> SchemaLike:
    """Return the Arrow schema with dataset metadata applied.

    Returns:
    -------
    SchemaLike
        Arrow schema with merged metadata.
    """
    ordering = _ordering_metadata_spec(spec.contract_spec, spec.table_spec)
    delta_policy = spec.policies.delta
    if (
        ordering is None
        and delta_policy is not None
        and delta_policy.write_policy is not None
        and delta_policy.write_policy.zorder_by
    ):
        keys = tuple((name, "ascending") for name in delta_policy.write_policy.zorder_by)
        ordering = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    merged = merge_metadata_specs(spec.metadata_spec, ordering)
    return merged.apply(spec.table_spec.to_arrow_schema())


def dataset_spec_ordering(spec: DatasetSpec) -> Ordering:
    """Return ordering metadata derived from the dataset spec.

    Returns:
    -------
    Ordering
        Ordering derived from contract and key fields.
    """
    if spec.contract_spec is not None and spec.contract_spec.canonical_sort:
        keys = tuple((key.column, key.order) for key in spec.contract_spec.canonical_sort)
        return Ordering.explicit(keys)
    if spec.table_spec.key_fields:
        return Ordering.implicit()
    return Ordering.unordered()


def dataset_spec_query(spec: DatasetSpec) -> QuerySpec:
    """Return the query spec, deriving it from the table spec if needed.

    Returns:
    -------
    QuerySpec
        Query specification for this dataset.
    """
    if spec.query_spec is not None:
        return spec.query_spec
    cols = tuple(field.name for field in spec.table_spec.fields)
    derived = {field_spec.name: field_spec.expr for field_spec in spec.derived_fields}
    return QuerySpec(
        projection=ProjectionSpec(base=cols, derived=derived),
        predicate=spec.predicate,
        pushdown_predicate=spec.pushdown_predicate,
    )


def dataset_spec_contract_spec_or_default(spec: DatasetSpec) -> ContractSpec:
    """Return the contract spec, deriving a default when missing.

    Returns:
    -------
    ContractSpec
        Contract spec derived or defaulted from the dataset spec.
    """
    if spec.contract_spec is not None:
        validation = spec.policies.validation
        if validation is None or spec.contract_spec.validation is not None:
            return spec.contract_spec
        return msgspec.structs.replace(spec.contract_spec, validation=validation)
    return ContractSpec(
        name=spec.table_spec.name,
        table_schema=spec.table_spec,
        version=spec.table_spec.version,
        validation=spec.policies.validation,
    )


def dataset_spec_contract(spec: DatasetSpec) -> Contract:
    """Return a runtime contract derived from the contract spec.

    Returns:
    -------
    Contract
        Runtime contract instance.
    """
    return dataset_spec_contract_spec_or_default(spec).to_contract()


def dataset_spec_handle(spec: DatasetSpec) -> DatasetHandle:
    """Return a DatasetHandle for this dataset spec.

    Returns:
    -------
    DatasetHandle
        Dataset handle for the dataset spec.
    """
    from schema_spec.dataset_handle import DatasetHandle

    return DatasetHandle(spec=spec)


def dataset_spec_resolved_view_specs(spec: DatasetSpec) -> tuple[ViewSpec, ...]:
    """Return the merged view specs for this dataset.

    Returns:
    -------
    tuple[ViewSpec, ...]
        View specs merged from contract and dataset scopes.
    """
    specs: list[ViewSpec] = []
    seen: set[str] = set()
    if spec.contract_spec is not None:
        for view in spec.contract_spec.view_specs:
            if view.name in seen:
                continue
            specs.append(view)
            seen.add(view.name)
    for view in spec.view_specs:
        if view.name in seen:
            continue
        specs.append(view)
        seen.add(view.name)
    return tuple(specs)


def dataset_spec_is_streaming(spec: DatasetSpec) -> bool:
    """Return True if this dataset represents a streaming source.

    Returns:
    -------
    bool
        ``True`` when the dataset is configured as unbounded.
    """
    if spec.policies.datafusion_scan is not None:
        return spec.policies.datafusion_scan.unbounded
    return False


def dataset_spec_finalize_context(spec: DatasetSpec) -> FinalizeContext:
    """Return a FinalizeContext for this dataset spec.

    Returns:
    -------
    FinalizeContext
        Finalize context with contract and schema policy.
    """
    contract = dataset_spec_contract(spec)
    ordering = _ordering_metadata_spec(spec.contract_spec, spec.table_spec)
    metadata = merge_metadata_specs(spec.metadata_spec, ordering)
    policy = schema_policy_factory(
        spec.table_spec,
        options=SchemaPolicyOptions(
            schema=contract.with_versioned_schema(),
            encoding=dataset_spec_encoding_policy(spec),
            metadata=metadata,
            validation=contract.validation,
        ),
    )
    return FinalizeContext(contract=contract, schema_policy=policy)


def dataset_spec_unify_tables(
    spec: DatasetSpec,
    tables: Sequence[TableLike],
    *,
    safe_cast: bool = True,
    keep_extra_columns: bool = False,
) -> TableLike:
    """Unify table schemas using the evolution spec and execution context.

    Returns:
    -------
    TableLike
        Unified table with aligned schema.
    """
    on_error: CastErrorPolicy = "unsafe" if safe_cast else "raise"
    return spec.evolution_spec.unify_and_cast(
        tables,
        safe_cast=safe_cast,
        on_error=on_error,
        keep_extra_columns=keep_extra_columns,
    )


def dataset_spec_encoding_policy(spec: DatasetSpec) -> EncodingPolicy | None:
    """Return an encoding policy derived from the schema spec.

    Returns:
    -------
    EncodingPolicy | None
        Encoding policy derived from the schema spec when defined.
    """
    policy = encoding_policy_from_spec(spec.table_spec)
    if not policy.specs:
        return None
    return policy


__all__ = [
    "dataset_spec_contract",
    "dataset_spec_contract_spec_or_default",
    "dataset_spec_dataframe_validation",
    "dataset_spec_datafusion_scan",
    "dataset_spec_delta_cdf_policy",
    "dataset_spec_delta_constraints",
    "dataset_spec_delta_feature_gate",
    "dataset_spec_delta_maintenance_policy",
    "dataset_spec_delta_scan",
    "dataset_spec_delta_schema_policy",
    "dataset_spec_delta_write_policy",
    "dataset_spec_encoding_policy",
    "dataset_spec_finalize_context",
    "dataset_spec_handle",
    "dataset_spec_is_streaming",
    "dataset_spec_name",
    "dataset_spec_ordering",
    "dataset_spec_query",
    "dataset_spec_resolved_view_specs",
    "dataset_spec_schema",
    "dataset_spec_unify_tables",
    "dataset_spec_validation",
]
