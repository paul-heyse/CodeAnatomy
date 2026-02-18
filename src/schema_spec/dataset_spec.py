"""Dataset-spec facade over canonical split modules and runtime adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.dataset_contracts import (
    ContractRow,
    DedupeSpecSpec,
    SortKeySpec,
    TableSchemaContract,
)
from schema_spec.scan_options import DataFusionScanOptions, DeltaScanOptions, ParquetColumnOptions
from schema_spec.scan_policy import (
    DeltaScanPolicyDefaults,
    ScanPolicyConfig,
    ScanPolicyDefaults,
    apply_delta_scan_policy,
    apply_scan_policy,
)
from schema_spec.specs import TableSchemaSpec

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> TableLike:
    """Validate an Arrow table against a schema specification.

    Returns:
    -------
    TableLike
        Validated table-like payload.
    """
    from schema_spec.validation import validate_arrow_table as _validate_arrow_table

    return _validate_arrow_table(
        table,
        spec=spec,
        options=options,
        runtime_profile=runtime_profile,
    )


from schema_spec import dataset_spec_runtime as _dataset_spec_runtime
from storage.deltalake import DeltaSchemaRequest

# Keep dataset_spec as a stable import surface by explicitly binding
# runtime-extracted symbols (instead of dynamic globals() updates).
ContractCatalogSpec = _dataset_spec_runtime.ContractCatalogSpec
ContractKwargs = _dataset_spec_runtime.ContractKwargs
ContractSpec = _dataset_spec_runtime.ContractSpec
ContractSpecKwargs = _dataset_spec_runtime.ContractSpecKwargs
DatasetKind = _dataset_spec_runtime.DatasetKind
DatasetOpenSpec = _dataset_spec_runtime.DatasetOpenSpec
DatasetPolicies = _dataset_spec_runtime.DatasetPolicies
DatasetSpec = _dataset_spec_runtime.DatasetSpec
DatasetSpecKwargs = _dataset_spec_runtime.DatasetSpecKwargs
DeltaCdfPolicy = _dataset_spec_runtime.DeltaCdfPolicy
DeltaMaintenancePolicy = _dataset_spec_runtime.DeltaMaintenancePolicy
DeltaPolicyBundle = _dataset_spec_runtime.DeltaPolicyBundle
DeltaSchemaPolicy = _dataset_spec_runtime.DeltaSchemaPolicy
DeltaWritePolicy = _dataset_spec_runtime.DeltaWritePolicy
TableSpecConstraints = _dataset_spec_runtime.TableSpecConstraints
ValidationPolicySpec = _dataset_spec_runtime.ValidationPolicySpec
VirtualFieldSpec = _dataset_spec_runtime.VirtualFieldSpec
dataset_spec_contract = _dataset_spec_runtime.dataset_spec_contract
dataset_spec_contract_spec_or_default = _dataset_spec_runtime.dataset_spec_contract_spec_or_default
dataset_spec_datafusion_scan = _dataset_spec_runtime.dataset_spec_datafusion_scan
dataset_spec_delta_cdf_policy = _dataset_spec_runtime.dataset_spec_delta_cdf_policy
dataset_spec_delta_constraints = _dataset_spec_runtime.dataset_spec_delta_constraints
dataset_spec_delta_feature_gate = _dataset_spec_runtime.dataset_spec_delta_feature_gate
dataset_spec_delta_maintenance_policy = _dataset_spec_runtime.dataset_spec_delta_maintenance_policy
dataset_spec_delta_scan = _dataset_spec_runtime.dataset_spec_delta_scan
dataset_spec_delta_schema_policy = _dataset_spec_runtime.dataset_spec_delta_schema_policy
dataset_spec_delta_write_policy = _dataset_spec_runtime.dataset_spec_delta_write_policy
dataset_spec_encoding_policy = _dataset_spec_runtime.dataset_spec_encoding_policy
dataset_spec_from_contract = _dataset_spec_runtime.dataset_spec_from_contract
dataset_spec_from_dataset = _dataset_spec_runtime.dataset_spec_from_dataset
dataset_spec_from_path = _dataset_spec_runtime.dataset_spec_from_path
dataset_spec_from_schema = _dataset_spec_runtime.dataset_spec_from_schema
dataset_spec_name = _dataset_spec_runtime.dataset_spec_name
dataset_spec_ordering = _dataset_spec_runtime.dataset_spec_ordering
dataset_spec_query = _dataset_spec_runtime.dataset_spec_query
dataset_spec_resolved_view_specs = _dataset_spec_runtime.dataset_spec_resolved_view_specs
dataset_spec_schema = _dataset_spec_runtime.dataset_spec_schema
dataset_spec_strict_schema_validation = _dataset_spec_runtime.dataset_spec_strict_schema_validation
dataset_spec_with_delta_maintenance = _dataset_spec_runtime.dataset_spec_with_delta_maintenance
dataset_table_column_defaults = _dataset_spec_runtime.dataset_table_column_defaults
dataset_table_constraints = _dataset_spec_runtime.dataset_table_constraints
dataset_table_ddl_fingerprint = _dataset_spec_runtime.dataset_table_ddl_fingerprint
dataset_table_definition = _dataset_spec_runtime.dataset_table_definition
dataset_table_logical_plan = _dataset_spec_runtime.dataset_table_logical_plan
ddl_fingerprint_from_definition = _dataset_spec_runtime.ddl_fingerprint_from_definition
make_contract_spec = _dataset_spec_runtime.make_contract_spec
make_dataset_spec = _dataset_spec_runtime.make_dataset_spec
make_table_spec = _dataset_spec_runtime.make_table_spec
resolve_schema_evolution_spec = _dataset_spec_runtime.resolve_schema_evolution_spec
schema_from_datafusion_catalog = _dataset_spec_runtime.schema_from_datafusion_catalog
SCHEMA_EVOLUTION_PRESETS = _dataset_spec_runtime.SCHEMA_EVOLUTION_PRESETS
table_spec_from_schema = _dataset_spec_runtime.table_spec_from_schema
validation_policy_payload = _dataset_spec_runtime.validation_policy_payload
validation_policy_to_arrow_options = _dataset_spec_runtime.validation_policy_to_arrow_options

__all__ = [
    "SCHEMA_EVOLUTION_PRESETS",
    "ArrowValidationOptions",
    "ContractCatalogSpec",
    "ContractKwargs",
    "ContractRow",
    "ContractSpec",
    "ContractSpecKwargs",
    "DataFusionScanOptions",
    "DatasetKind",
    "DatasetOpenSpec",
    "DatasetPolicies",
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "DeltaCdfPolicy",
    "DeltaMaintenancePolicy",
    "DeltaPolicyBundle",
    "DeltaScanOptions",
    "DeltaScanPolicyDefaults",
    "DeltaSchemaPolicy",
    "DeltaSchemaRequest",
    "DeltaWritePolicy",
    "ParquetColumnOptions",
    "ScanPolicyConfig",
    "ScanPolicyDefaults",
    "SortKeySpec",
    "TableSchemaContract",
    "TableSpecConstraints",
    "ValidationPolicySpec",
    "VirtualFieldSpec",
    "apply_delta_scan_policy",
    "apply_scan_policy",
    "dataset_spec_contract",
    "dataset_spec_contract_spec_or_default",
    "dataset_spec_datafusion_scan",
    "dataset_spec_delta_cdf_policy",
    "dataset_spec_delta_constraints",
    "dataset_spec_delta_feature_gate",
    "dataset_spec_delta_maintenance_policy",
    "dataset_spec_delta_scan",
    "dataset_spec_delta_schema_policy",
    "dataset_spec_delta_write_policy",
    "dataset_spec_encoding_policy",
    "dataset_spec_from_contract",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "dataset_spec_name",
    "dataset_spec_ordering",
    "dataset_spec_query",
    "dataset_spec_resolved_view_specs",
    "dataset_spec_schema",
    "dataset_spec_strict_schema_validation",
    "dataset_spec_with_delta_maintenance",
    "dataset_table_column_defaults",
    "dataset_table_constraints",
    "dataset_table_ddl_fingerprint",
    "dataset_table_definition",
    "dataset_table_logical_plan",
    "ddl_fingerprint_from_definition",
    "make_contract_spec",
    "make_dataset_spec",
    "make_table_spec",
    "resolve_schema_evolution_spec",
    "schema_from_datafusion_catalog",
    "table_spec_from_schema",
    "validate_arrow_table",
    "validation_policy_payload",
    "validation_policy_to_arrow_options",
]
