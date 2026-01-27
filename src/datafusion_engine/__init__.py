"""DataFusion execution helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.catalog_provider import (
        RegistryCatalogProvider,
        RegistrySchemaProvider,
        register_registry_catalog,
    )
    from datafusion_engine.compile_options import (
        DataFusionCompileOptions,
        DataFusionSqlPolicy,
    )
    from datafusion_engine.delta_store_policy import DeltaStorePolicy
    from datafusion_engine.diagnostics import (
        DiagnosticsContext,
        DiagnosticsRecorder,
        DiagnosticsSink,
        InMemoryDiagnosticsSink,
    )
    from datafusion_engine.execution_facade import (
        DataFusionExecutionFacade,
        ExecutionResult,
        ExecutionResultKind,
    )
    from datafusion_engine.introspection import (
        IntrospectionCache,
        IntrospectionSnapshot,
    )
    from datafusion_engine.io_adapter import DataFusionIOAdapter
    from datafusion_engine.lineage_datafusion import (
        LineageReport,
        ScanLineage,
        extract_lineage,
        referenced_tables_from_plan,
        required_columns_by_table,
    )
    from datafusion_engine.param_binding import (
        DataFusionParamBindings,
        apply_bindings_to_context,
        resolve_param_bindings,
    )
    from datafusion_engine.plan_bundle import (
        DataFusionPlanBundle,
        build_plan_bundle,
    )
    from datafusion_engine.plan_udf_analysis import (
        derive_required_udfs_from_plans,
        ensure_plan_udfs_available,
        extract_udfs_from_dataframe,
        extract_udfs_from_logical_plan,
        extract_udfs_from_plan_bundle,
        validate_required_udfs_from_bundle,
        validate_required_udfs_from_plan,
    )
    from datafusion_engine.registry_loader import (
        RegistryTarget,
        register_registry_delta_tables,
        register_registry_exports,
        registry_delta_table_paths,
        registry_output_dir,
    )
    from datafusion_engine.runtime import (
        DEFAULT_DF_POLICY,
        SCHEMA_HARDENING_PRESETS,
        AdapterExecutionPolicy,
        DataFusionConfigPolicy,
        DataFusionRuntimeProfile,
        ExecutionLabel,
        MemoryPool,
        SchemaHardeningProfile,
        apply_execution_label,
        apply_execution_policy,
        register_view_specs,
        snapshot_plans,
    )
    from datafusion_engine.schema_contracts import (
        ColumnContract,
        ContractRegistry,
        EvolutionPolicy,
        SchemaContract,
        SchemaViolation,
        SchemaViolationType,
    )
    from datafusion_engine.schema_introspection import SchemaIntrospector
    from datafusion_engine.schema_registry import (
        extract_nested_dataset_names,
        extract_nested_schema_names,
        nested_base_df,
        nested_dataset_names,
        nested_schema_names,
    )
    from datafusion_engine.semantic_diff import (
        ChangeCategory,
        RebuildPolicy,
        SemanticChange,
        SemanticDiff,
        compute_rebuild_needed,
    )
    from datafusion_engine.streaming_executor import StreamingExecutionResult
    from datafusion_engine.udf_platform import (
        RustUdfPlatform,
        RustUdfPlatformOptions,
        install_rust_udf_platform,
    )
    from datafusion_engine.write_pipeline import (
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )

__all__ = [
    "DEFAULT_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    "ChangeCategory",
    "ColumnContract",
    "ContractRegistry",
    "DataFusionCompileOptions",
    "DataFusionConfigPolicy",
    "DataFusionExecutionFacade",
    "DataFusionIOAdapter",
    "DataFusionParamBindings",
    "DataFusionPlanBundle",
    "DataFusionRuntimeProfile",
    "DataFusionSqlPolicy",
    "DeltaStorePolicy",
    "DiagnosticsContext",
    "DiagnosticsRecorder",
    "DiagnosticsSink",
    "EvolutionPolicy",
    "ExecutionLabel",
    "ExecutionResult",
    "ExecutionResultKind",
    "InMemoryDiagnosticsSink",
    "IntrospectionCache",
    "IntrospectionSnapshot",
    "LineageReport",
    "MemoryPool",
    "RebuildPolicy",
    "RegistryCatalogProvider",
    "RegistrySchemaProvider",
    "RegistryTarget",
    "RustUdfPlatform",
    "RustUdfPlatformOptions",
    "ScanLineage",
    "SchemaContract",
    "SchemaHardeningProfile",
    "SchemaIntrospector",
    "SchemaViolation",
    "SchemaViolationType",
    "SemanticChange",
    "SemanticDiff",
    "StreamingExecutionResult",
    "WriteFormat",
    "WriteMode",
    "WritePipeline",
    "WriteRequest",
    "apply_bindings_to_context",
    "apply_execution_label",
    "apply_execution_policy",
    "build_plan_bundle",
    "compute_rebuild_needed",
    "derive_required_udfs_from_plans",
    "ensure_plan_udfs_available",
    "extract_lineage",
    "extract_nested_dataset_names",
    "extract_nested_schema_names",
    "extract_udfs_from_dataframe",
    "extract_udfs_from_logical_plan",
    "extract_udfs_from_plan_bundle",
    "install_rust_udf_platform",
    "nested_base_df",
    "nested_dataset_names",
    "nested_schema_names",
    "referenced_tables_from_plan",
    "register_registry_catalog",
    "register_registry_delta_tables",
    "register_registry_exports",
    "register_view_specs",
    "registry_delta_table_paths",
    "registry_output_dir",
    "required_columns_by_table",
    "resolve_param_bindings",
    "snapshot_plans",
    "validate_required_udfs_from_bundle",
    "validate_required_udfs_from_plan",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    # Runtime and configuration
    "DEFAULT_DF_POLICY": ("datafusion_engine.runtime", "DEFAULT_DF_POLICY"),
    "SCHEMA_HARDENING_PRESETS": ("datafusion_engine.runtime", "SCHEMA_HARDENING_PRESETS"),
    "AdapterExecutionPolicy": ("datafusion_engine.runtime", "AdapterExecutionPolicy"),
    "ExecutionLabel": ("datafusion_engine.runtime", "ExecutionLabel"),
    "DataFusionCompileOptions": ("datafusion_engine.compile_options", "DataFusionCompileOptions"),
    "DataFusionConfigPolicy": ("datafusion_engine.runtime", "DataFusionConfigPolicy"),
    "DataFusionRuntimeProfile": ("datafusion_engine.runtime", "DataFusionRuntimeProfile"),
    "DeltaStorePolicy": ("datafusion_engine.delta_store_policy", "DeltaStorePolicy"),
    "SchemaHardeningProfile": ("datafusion_engine.runtime", "SchemaHardeningProfile"),
    "DataFusionSqlPolicy": ("datafusion_engine.compile_options", "DataFusionSqlPolicy"),
    "MemoryPool": ("datafusion_engine.runtime", "MemoryPool"),
    "apply_execution_label": ("datafusion_engine.runtime", "apply_execution_label"),
    "apply_execution_policy": ("datafusion_engine.runtime", "apply_execution_policy"),
    "register_view_specs": ("datafusion_engine.runtime", "register_view_specs"),
    "snapshot_plans": ("datafusion_engine.runtime", "snapshot_plans"),
    # Catalog and schema
    "RegistryCatalogProvider": ("datafusion_engine.catalog_provider", "RegistryCatalogProvider"),
    "RegistrySchemaProvider": ("datafusion_engine.catalog_provider", "RegistrySchemaProvider"),
    "register_registry_catalog": (
        "datafusion_engine.catalog_provider",
        "register_registry_catalog",
    ),
    "nested_base_df": ("datafusion_engine.schema_registry", "nested_base_df"),
    "nested_dataset_names": ("datafusion_engine.schema_registry", "nested_dataset_names"),
    "nested_schema_names": ("datafusion_engine.schema_registry", "nested_schema_names"),
    "extract_nested_dataset_names": (
        "datafusion_engine.schema_registry",
        "extract_nested_dataset_names",
    ),
    "extract_nested_schema_names": (
        "datafusion_engine.schema_registry",
        "extract_nested_schema_names",
    ),
    "register_registry_delta_tables": (
        "datafusion_engine.registry_loader",
        "register_registry_delta_tables",
    ),
    "register_registry_exports": (
        "datafusion_engine.registry_loader",
        "register_registry_exports",
    ),
    "registry_delta_table_paths": (
        "datafusion_engine.registry_loader",
        "registry_delta_table_paths",
    ),
    "registry_output_dir": ("datafusion_engine.registry_loader", "registry_output_dir"),
    "RegistryTarget": ("datafusion_engine.registry_loader", "RegistryTarget"),
    "SchemaIntrospector": ("datafusion_engine.schema_introspection", "SchemaIntrospector"),
    # Bridge and execution
    # Streaming Execution
    "StreamingExecutionResult": (
        "datafusion_engine.streaming_executor",
        "StreamingExecutionResult",
    ),
    # Parameter Binding
    "DataFusionParamBindings": ("datafusion_engine.param_binding", "DataFusionParamBindings"),
    "apply_bindings_to_context": (
        "datafusion_engine.param_binding",
        "apply_bindings_to_context",
    ),
    "resolve_param_bindings": ("datafusion_engine.param_binding", "resolve_param_bindings"),
    # SQL Safety
    "ExecutionPolicy": ("datafusion_engine.sql_safety", "ExecutionPolicy"),
    "SafeExecutor": ("datafusion_engine.sql_safety", "SafeExecutor"),
    "execute_with_policy": ("datafusion_engine.sql_safety", "execute_with_policy"),
    "validate_sql_safety": ("datafusion_engine.sql_safety", "validate_sql_safety"),
    # Introspection
    "IntrospectionCache": ("datafusion_engine.introspection", "IntrospectionCache"),
    "IntrospectionSnapshot": ("datafusion_engine.introspection", "IntrospectionSnapshot"),
    # Diagnostics
    "DiagnosticsContext": ("datafusion_engine.diagnostics", "DiagnosticsContext"),
    "DiagnosticsRecorder": ("datafusion_engine.diagnostics", "DiagnosticsRecorder"),
    "DiagnosticsSink": ("datafusion_engine.diagnostics", "DiagnosticsSink"),
    "InMemoryDiagnosticsSink": ("datafusion_engine.diagnostics", "InMemoryDiagnosticsSink"),
    # Execution Facade
    "CompiledPlan": ("datafusion_engine.execution_facade", "CompiledPlan"),
    "DataFusionExecutionFacade": (
        "datafusion_engine.execution_facade",
        "DataFusionExecutionFacade",
    ),
    "ExecutionResult": ("datafusion_engine.execution_facade", "ExecutionResult"),
    "ExecutionResultKind": ("datafusion_engine.execution_facade", "ExecutionResultKind"),
    # IO Adapter
    "DataFusionIOAdapter": ("datafusion_engine.io_adapter", "DataFusionIOAdapter"),
    # Write Pipeline
    "WriteFormat": ("datafusion_engine.write_pipeline", "WriteFormat"),
    "WriteMode": ("datafusion_engine.write_pipeline", "WriteMode"),
    "WritePipeline": ("datafusion_engine.write_pipeline", "WritePipeline"),
    "WriteRequest": ("datafusion_engine.write_pipeline", "WriteRequest"),
    # Semantic Diff
    "ChangeCategory": ("datafusion_engine.semantic_diff", "ChangeCategory"),
    "RebuildPolicy": ("datafusion_engine.semantic_diff", "RebuildPolicy"),
    "SemanticChange": ("datafusion_engine.semantic_diff", "SemanticChange"),
    "SemanticDiff": ("datafusion_engine.semantic_diff", "SemanticDiff"),
    "compute_rebuild_needed": ("datafusion_engine.semantic_diff", "compute_rebuild_needed"),
    # Schema Contracts
    "ColumnContract": ("datafusion_engine.schema_contracts", "ColumnContract"),
    "ContractRegistry": ("datafusion_engine.schema_contracts", "ContractRegistry"),
    "EvolutionPolicy": ("datafusion_engine.schema_contracts", "EvolutionPolicy"),
    "SchemaContract": ("datafusion_engine.schema_contracts", "SchemaContract"),
    "SchemaViolation": ("datafusion_engine.schema_contracts", "SchemaViolation"),
    "SchemaViolationType": ("datafusion_engine.schema_contracts", "SchemaViolationType"),
    # Plan Bundle and Lineage (DataFusion-native)
    "DataFusionPlanBundle": ("datafusion_engine.plan_bundle", "DataFusionPlanBundle"),
    "build_plan_bundle": ("datafusion_engine.plan_bundle", "build_plan_bundle"),
    "LineageReport": ("datafusion_engine.lineage_datafusion", "LineageReport"),
    "ScanLineage": ("datafusion_engine.lineage_datafusion", "ScanLineage"),
    "extract_lineage": ("datafusion_engine.lineage_datafusion", "extract_lineage"),
    "referenced_tables_from_plan": (
        "datafusion_engine.lineage_datafusion",
        "referenced_tables_from_plan",
    ),
    "required_columns_by_table": (
        "datafusion_engine.lineage_datafusion",
        "required_columns_by_table",
    ),
    # Plan UDF Analysis
    "derive_required_udfs_from_plans": (
        "datafusion_engine.plan_udf_analysis",
        "derive_required_udfs_from_plans",
    ),
    "ensure_plan_udfs_available": (
        "datafusion_engine.plan_udf_analysis",
        "ensure_plan_udfs_available",
    ),
    "extract_udfs_from_dataframe": (
        "datafusion_engine.plan_udf_analysis",
        "extract_udfs_from_dataframe",
    ),
    "extract_udfs_from_logical_plan": (
        "datafusion_engine.plan_udf_analysis",
        "extract_udfs_from_logical_plan",
    ),
    "extract_udfs_from_plan_bundle": (
        "datafusion_engine.plan_udf_analysis",
        "extract_udfs_from_plan_bundle",
    ),
    "validate_required_udfs_from_bundle": (
        "datafusion_engine.plan_udf_analysis",
        "validate_required_udfs_from_bundle",
    ),
    "validate_required_udfs_from_plan": (
        "datafusion_engine.plan_udf_analysis",
        "validate_required_udfs_from_plan",
    ),
    # UDF Platform (Planning-Critical Extensions)
    "RustUdfPlatform": ("datafusion_engine.udf_platform", "RustUdfPlatform"),
    "RustUdfPlatformOptions": ("datafusion_engine.udf_platform", "RustUdfPlatformOptions"),
    "install_rust_udf_platform": ("datafusion_engine.udf_platform", "install_rust_udf_platform"),
}


def __getattr__(name: str) -> object:
    if name not in _EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = _EXPORTS[name]
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
