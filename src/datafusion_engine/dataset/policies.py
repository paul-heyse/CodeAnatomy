"""Centralized dataset policy resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass

import msgspec

from arrow_utils.core.ordering import OrderingLevel
from schema_spec.dataset_spec import (
    ArrowValidationOptions,
    DataFusionScanOptions,
    DatasetSpec,
    DeltaPolicyBundle,
    DeltaScanOptions,
    ScanPolicyConfig,
    ValidationPolicySpec,
)
from serde_msgspec import StructBaseStrict


@dataclass(frozen=True, slots=True)
class ResolvedDatasetPolicies:
    """Resolved dataset policy bundle."""

    datafusion_scan: DataFusionScanOptions | None
    delta_scan: DeltaScanOptions | None
    delta_bundle: DeltaPolicyBundle | None
    validation: ArrowValidationOptions | None
    dataframe_validation: ValidationPolicySpec | None


class DatasetPolicyRequest(StructBaseStrict, frozen=True):
    """Inputs for resolving dataset policies."""

    dataset_format: str
    dataset_spec: DatasetSpec | None = None
    datafusion_scan_override: DataFusionScanOptions | None = None
    delta_policy_override: DeltaPolicyBundle | None = None
    validation_override: ArrowValidationOptions | None = None
    dataframe_validation_override: ValidationPolicySpec | None = None


_DEFAULT_DELTA_SCAN = DeltaScanOptions(
    file_column_name="__delta_rs_path",
    enable_parquet_pushdown=True,
    schema_force_view_types=True,
    wrap_partition_values=True,
    schema=None,
)


def resolve_dataset_policies(request: DatasetPolicyRequest) -> ResolvedDatasetPolicies:
    """Resolve dataset policies using precedence rules.

    Returns:
    -------
    ResolvedDatasetPolicies
        Resolved policy bundle for the dataset.
    """
    datafusion_scan = resolve_datafusion_scan_options(
        dataset_spec=request.dataset_spec,
        override=request.datafusion_scan_override,
    )
    delta_scan = resolve_delta_scan_options(
        dataset_format=request.dataset_format,
        dataset_spec=request.dataset_spec,
        override=(
            request.delta_policy_override.scan
            if request.delta_policy_override is not None
            else None
        ),
    )
    delta_bundle = resolve_delta_policy_bundle(
        dataset_spec=request.dataset_spec,
        override=request.delta_policy_override,
    )
    validation, dataframe_validation = resolve_validation_policies(
        dataset_spec=request.dataset_spec,
        validation_override=request.validation_override,
        dataframe_validation_override=request.dataframe_validation_override,
    )
    return ResolvedDatasetPolicies(
        datafusion_scan=datafusion_scan,
        delta_scan=delta_scan,
        delta_bundle=delta_bundle,
        validation=validation,
        dataframe_validation=dataframe_validation,
    )


def resolve_validation_policies(
    *,
    dataset_spec: DatasetSpec | None,
    validation_override: ArrowValidationOptions | None,
    dataframe_validation_override: ValidationPolicySpec | None,
) -> tuple[ArrowValidationOptions | None, ValidationPolicySpec | None]:
    """Resolve validation policies with override precedence.

    Returns:
    -------
    tuple[ArrowValidationOptions | None, ValidationPolicySpec | None]
        Validation and dataframe-validation policies.
    """
    validation = validation_override
    if validation is None and dataset_spec is not None:
        validation = dataset_spec.policies.validation
    dataframe_validation = dataframe_validation_override
    if dataframe_validation is None and dataset_spec is not None:
        dataframe_validation = dataset_spec.policies.dataframe_validation
    return validation, dataframe_validation


def resolve_datafusion_scan_options(
    *,
    dataset_spec: DatasetSpec | None,
    override: DataFusionScanOptions | None,
) -> DataFusionScanOptions | None:
    """Resolve DataFusion scan options with ordering defaults.

    Returns:
    -------
    DataFusionScanOptions | None
        Resolved DataFusion scan options.
    """
    from schema_spec.dataset_spec import dataset_spec_datafusion_scan, dataset_spec_ordering

    scan = override or (
        dataset_spec_datafusion_scan(dataset_spec) if dataset_spec is not None else None
    )
    if scan is None:
        return None
    if dataset_spec is None:
        return scan
    if scan.file_sort_order:
        return scan
    ordering = dataset_spec_ordering(dataset_spec)
    file_sort_order: tuple[tuple[str, str], ...] = ()
    if ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        file_sort_order = tuple(ordering.keys)
    elif dataset_spec.table_spec.key_fields:
        file_sort_order = tuple((name, "ascending") for name in dataset_spec.table_spec.key_fields)
    if not file_sort_order:
        return scan
    return msgspec.structs.replace(scan, file_sort_order=file_sort_order)


def resolve_delta_policy_bundle(
    *,
    dataset_spec: DatasetSpec | None,
    override: DeltaPolicyBundle | None,
) -> DeltaPolicyBundle | None:
    """Resolve Delta policy bundle with override precedence.

    Returns:
    -------
    DeltaPolicyBundle | None
        Resolved Delta policy bundle.
    """
    base = dataset_spec.policies.delta if dataset_spec is not None else None
    return merge_delta_policy_bundle(base, override)


def merge_delta_policy_bundle(
    base: DeltaPolicyBundle | None,
    override: DeltaPolicyBundle | None,
) -> DeltaPolicyBundle | None:
    """Merge Delta policy bundles with override precedence.

    Returns:
    -------
    DeltaPolicyBundle | None
        Merged policy bundle.
    """
    if override is None:
        return base
    if base is None:
        return override
    from schema_spec.dataset_spec import DeltaPolicyBundle as _DeltaPolicyBundle

    return _DeltaPolicyBundle(
        scan=override.scan or base.scan,
        cdf_policy=override.cdf_policy or base.cdf_policy,
        maintenance_policy=override.maintenance_policy or base.maintenance_policy,
        write_policy=override.write_policy or base.write_policy,
        schema_policy=override.schema_policy or base.schema_policy,
        feature_gate=override.feature_gate or base.feature_gate,
        constraints=override.constraints,
    )


def resolve_delta_scan_options(
    *,
    dataset_format: str,
    dataset_spec: DatasetSpec | None,
    override: DeltaScanOptions | None,
) -> DeltaScanOptions | None:
    """Resolve Delta scan options with defaults and overrides.

    Returns:
    -------
    DeltaScanOptions | None
        Resolved Delta scan options.
    """
    if dataset_format != "delta":
        return None
    base = _DEFAULT_DELTA_SCAN
    if dataset_spec is not None:
        from schema_spec.dataset_spec import dataset_spec_delta_scan

        delta_scan = dataset_spec_delta_scan(dataset_spec)
        if delta_scan is not None:
            base = _merge_delta_scan(base, delta_scan)
    return _merge_delta_scan(base, override)


def apply_scan_policy_defaults(
    *,
    dataset_format: str,
    datafusion_scan: DataFusionScanOptions | None,
    delta_scan: DeltaScanOptions | None,
    policy: ScanPolicyConfig | None,
) -> tuple[DataFusionScanOptions | None, DeltaScanOptions | None]:
    """Apply scan policy defaults to scan options.

    Returns:
    -------
    tuple[DataFusionScanOptions | None, DeltaScanOptions | None]
        Updated scan options after applying the scan policy.
    """
    if policy is None:
        return datafusion_scan, delta_scan
    from schema_spec.scan_policy import apply_delta_scan_policy, apply_scan_policy

    datafusion_scan = apply_scan_policy(
        datafusion_scan,
        policy=policy,
        dataset_format=dataset_format,
    )
    if dataset_format == "delta":
        delta_scan = apply_delta_scan_policy(delta_scan, policy=policy)
    return datafusion_scan, delta_scan


def _merge_delta_scan(
    base: DeltaScanOptions,
    override: DeltaScanOptions | None,
) -> DeltaScanOptions:
    if override is None:
        return base
    return DeltaScanOptions(
        file_column_name=override.file_column_name or base.file_column_name,
        enable_parquet_pushdown=(
            override.enable_parquet_pushdown
            if override.enable_parquet_pushdown is not None
            else base.enable_parquet_pushdown
        ),
        schema_force_view_types=(
            override.schema_force_view_types
            if override.schema_force_view_types is not None
            else base.schema_force_view_types
        ),
        wrap_partition_values=(
            override.wrap_partition_values
            if override.wrap_partition_values is not None
            else base.wrap_partition_values
        ),
        schema=override.schema or base.schema,
    )


__all__ = [
    "DatasetPolicyRequest",
    "ResolvedDatasetPolicies",
    "apply_scan_policy_defaults",
    "merge_delta_policy_bundle",
    "resolve_datafusion_scan_options",
    "resolve_dataset_policies",
    "resolve_delta_policy_bundle",
    "resolve_delta_scan_options",
    "resolve_validation_policies",
]
