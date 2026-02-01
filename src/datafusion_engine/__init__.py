"""DataFusion execution helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.catalog.introspection import (
        IntrospectionCache,
        IntrospectionSnapshot,
    )
    from datafusion_engine.catalog.provider import (
        RegistryCatalogProvider,
        RegistrySchemaProvider,
        register_registry_catalog,
    )
    from datafusion_engine.compile.options import (
        DataFusionCompileOptions,
        DataFusionSqlPolicy,
    )
    from datafusion_engine.delta.store_policy import DeltaStorePolicy
    from datafusion_engine.errors import (
        DataFusionEngineError,
        ErrorKind,
    )
    from datafusion_engine.io.adapter import DataFusionIOAdapter
    from datafusion_engine.io.write import (
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )
    from datafusion_engine.lineage.datafusion import (
        LineageReport,
        ScanLineage,
        extract_lineage,
        referenced_tables_from_plan,
        required_columns_by_table,
    )
    from datafusion_engine.lineage.diagnostics import (
        DiagnosticsContext,
        DiagnosticsRecorder,
        DiagnosticsSink,
        InMemoryDiagnosticsSink,
    )
    from datafusion_engine.plan.bundle import (
        DataFusionPlanBundle,
        build_plan_bundle,
    )
    from datafusion_engine.plan.execution import (
        PlanExecutionOptions,
        PlanExecutionResult,
        execute_plan_bundle,
    )
    from datafusion_engine.plan.udf_analysis import (
        derive_required_udfs_from_plans,
        ensure_plan_udfs_available,
        extract_udfs_from_dataframe,
        extract_udfs_from_logical_plan,
        extract_udfs_from_plan_bundle,
        validate_required_udfs_from_bundle,
        validate_required_udfs_from_plan,
    )
    from datafusion_engine.schema.contracts import (
        ContractRegistry,
        EvolutionPolicy,
        SchemaContract,
        ValidationViolation,
        ViolationType,
    )
    from datafusion_engine.schema.introspection import SchemaIntrospector
    from datafusion_engine.schema.registry import (
        extract_nested_dataset_names,
        extract_nested_schema_names,
        nested_base_df,
        nested_dataset_names,
        nested_schema_names,
    )
    from datafusion_engine.session.facade import (
        DataFusionExecutionFacade,
        ExecutionResult,
        ExecutionResultKind,
    )
    from datafusion_engine.session.runtime import (
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
    )
    from datafusion_engine.session.streaming import StreamingExecutionResult
    from datafusion_engine.tables.param import (
        DataFusionParamBindings,
        apply_bindings_to_context,
        resolve_param_bindings,
    )
    from datafusion_engine.udf.platform import (
        RustUdfPlatform,
        RustUdfPlatformOptions,
        install_rust_udf_platform,
    )

__all__ = [
    "DEFAULT_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    "ContractRegistry",
    "DataFusionCompileOptions",
    "DataFusionConfigPolicy",
    "DataFusionEngineError",
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
    "ErrorKind",
    "EvolutionPolicy",
    "ExecutionLabel",
    "ExecutionResult",
    "ExecutionResultKind",
    "InMemoryDiagnosticsSink",
    "IntrospectionCache",
    "IntrospectionSnapshot",
    "LineageReport",
    "MemoryPool",
    "PlanExecutionOptions",
    "PlanExecutionResult",
    "RegistryCatalogProvider",
    "RegistrySchemaProvider",
    "RustUdfPlatform",
    "RustUdfPlatformOptions",
    "ScanLineage",
    "SchemaContract",
    "SchemaHardeningProfile",
    "SchemaIntrospector",
    "StreamingExecutionResult",
    "ValidationViolation",
    "ViolationType",
    "WriteFormat",
    "WriteMode",
    "WritePipeline",
    "WriteRequest",
    "apply_bindings_to_context",
    "apply_execution_label",
    "apply_execution_policy",
    "build_plan_bundle",
    "derive_required_udfs_from_plans",
    "ensure_plan_udfs_available",
    "execute_plan_bundle",
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
    "required_columns_by_table",
    "resolve_param_bindings",
    "validate_required_udfs_from_bundle",
    "validate_required_udfs_from_plan",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    # Runtime and configuration
    "DEFAULT_DF_POLICY": ("datafusion_engine.session.runtime", "DEFAULT_DF_POLICY"),
    "SCHEMA_HARDENING_PRESETS": ("datafusion_engine.session.runtime", "SCHEMA_HARDENING_PRESETS"),
    "AdapterExecutionPolicy": ("datafusion_engine.session.runtime", "AdapterExecutionPolicy"),
    "ExecutionLabel": ("datafusion_engine.session.runtime", "ExecutionLabel"),
    "DataFusionCompileOptions": ("datafusion_engine.compile.options", "DataFusionCompileOptions"),
    "DataFusionConfigPolicy": ("datafusion_engine.session.runtime", "DataFusionConfigPolicy"),
    "DataFusionEngineError": ("datafusion_engine.errors", "DataFusionEngineError"),
    "DataFusionRuntimeProfile": ("datafusion_engine.session.runtime", "DataFusionRuntimeProfile"),
    "DeltaStorePolicy": ("datafusion_engine.delta.store_policy", "DeltaStorePolicy"),
    "ErrorKind": ("datafusion_engine.errors", "ErrorKind"),
    "SchemaHardeningProfile": ("datafusion_engine.session.runtime", "SchemaHardeningProfile"),
    "DataFusionSqlPolicy": ("datafusion_engine.compile.options", "DataFusionSqlPolicy"),
    "MemoryPool": ("datafusion_engine.session.runtime", "MemoryPool"),
    "apply_execution_label": ("datafusion_engine.session.runtime", "apply_execution_label"),
    "apply_execution_policy": ("datafusion_engine.session.runtime", "apply_execution_policy"),
    # Catalog and schema
    "RegistryCatalogProvider": ("datafusion_engine.catalog.provider", "RegistryCatalogProvider"),
    "RegistrySchemaProvider": ("datafusion_engine.catalog.provider", "RegistrySchemaProvider"),
    "register_registry_catalog": (
        "datafusion_engine.catalog.provider",
        "register_registry_catalog",
    ),
    "nested_base_df": ("datafusion_engine.schema.registry", "nested_base_df"),
    "nested_dataset_names": ("datafusion_engine.schema.registry", "nested_dataset_names"),
    "nested_schema_names": ("datafusion_engine.schema.registry", "nested_schema_names"),
    "extract_nested_dataset_names": (
        "datafusion_engine.schema.registry",
        "extract_nested_dataset_names",
    ),
    "extract_nested_schema_names": (
        "datafusion_engine.schema.registry",
        "extract_nested_schema_names",
    ),
    "SchemaIntrospector": ("datafusion_engine.schema.introspection", "SchemaIntrospector"),
    # Bridge and execution
    # Streaming Execution
    "StreamingExecutionResult": (
        "datafusion_engine.session.streaming",
        "StreamingExecutionResult",
    ),
    # Parameter Binding
    "DataFusionParamBindings": ("datafusion_engine.tables.param", "DataFusionParamBindings"),
    "apply_bindings_to_context": (
        "datafusion_engine.tables.param",
        "apply_bindings_to_context",
    ),
    "resolve_param_bindings": ("datafusion_engine.tables.param", "resolve_param_bindings"),
    # Introspection
    "IntrospectionCache": ("datafusion_engine.catalog.introspection", "IntrospectionCache"),
    "IntrospectionSnapshot": ("datafusion_engine.catalog.introspection", "IntrospectionSnapshot"),
    # Diagnostics
    "DiagnosticsContext": ("datafusion_engine.lineage.diagnostics", "DiagnosticsContext"),
    "DiagnosticsRecorder": ("datafusion_engine.lineage.diagnostics", "DiagnosticsRecorder"),
    "DiagnosticsSink": ("datafusion_engine.lineage.diagnostics", "DiagnosticsSink"),
    "InMemoryDiagnosticsSink": ("datafusion_engine.lineage.diagnostics", "InMemoryDiagnosticsSink"),
    # Execution Facade
    "DataFusionExecutionFacade": (
        "datafusion_engine.session.facade",
        "DataFusionExecutionFacade",
    ),
    "ExecutionResult": ("datafusion_engine.session.facade", "ExecutionResult"),
    "ExecutionResultKind": ("datafusion_engine.session.facade", "ExecutionResultKind"),
    # IO Adapter
    "DataFusionIOAdapter": ("datafusion_engine.io.adapter", "DataFusionIOAdapter"),
    # Write Pipeline
    "WriteFormat": ("datafusion_engine.io.write", "WriteFormat"),
    "WriteMode": ("datafusion_engine.io.write", "WriteMode"),
    "WritePipeline": ("datafusion_engine.io.write", "WritePipeline"),
    "WriteRequest": ("datafusion_engine.io.write", "WriteRequest"),
    # Schema Contracts
    "ContractRegistry": ("datafusion_engine.schema.contracts", "ContractRegistry"),
    "EvolutionPolicy": ("datafusion_engine.schema.contracts", "EvolutionPolicy"),
    "SchemaContract": ("datafusion_engine.schema.contracts", "SchemaContract"),
    "ValidationViolation": ("datafusion_engine.schema.contracts", "ValidationViolation"),
    "ViolationType": ("datafusion_engine.schema.contracts", "ViolationType"),
    # Plan Bundle and Lineage (DataFusion-native)
    "DataFusionPlanBundle": ("datafusion_engine.plan.bundle", "DataFusionPlanBundle"),
    "build_plan_bundle": ("datafusion_engine.plan.bundle", "build_plan_bundle"),
    "PlanExecutionOptions": ("datafusion_engine.plan.execution", "PlanExecutionOptions"),
    "PlanExecutionResult": ("datafusion_engine.plan.execution", "PlanExecutionResult"),
    "execute_plan_bundle": ("datafusion_engine.plan.execution", "execute_plan_bundle"),
    "LineageReport": ("datafusion_engine.lineage.datafusion", "LineageReport"),
    "ScanLineage": ("datafusion_engine.lineage.datafusion", "ScanLineage"),
    "extract_lineage": ("datafusion_engine.lineage.datafusion", "extract_lineage"),
    "referenced_tables_from_plan": (
        "datafusion_engine.lineage.datafusion",
        "referenced_tables_from_plan",
    ),
    "required_columns_by_table": (
        "datafusion_engine.lineage.datafusion",
        "required_columns_by_table",
    ),
    # Plan UDF Analysis
    "derive_required_udfs_from_plans": (
        "datafusion_engine.plan.udf_analysis",
        "derive_required_udfs_from_plans",
    ),
    "ensure_plan_udfs_available": (
        "datafusion_engine.plan.udf_analysis",
        "ensure_plan_udfs_available",
    ),
    "extract_udfs_from_dataframe": (
        "datafusion_engine.plan.udf_analysis",
        "extract_udfs_from_dataframe",
    ),
    "extract_udfs_from_logical_plan": (
        "datafusion_engine.plan.udf_analysis",
        "extract_udfs_from_logical_plan",
    ),
    "extract_udfs_from_plan_bundle": (
        "datafusion_engine.plan.udf_analysis",
        "extract_udfs_from_plan_bundle",
    ),
    "validate_required_udfs_from_bundle": (
        "datafusion_engine.plan.udf_analysis",
        "validate_required_udfs_from_bundle",
    ),
    "validate_required_udfs_from_plan": (
        "datafusion_engine.plan.udf_analysis",
        "validate_required_udfs_from_plan",
    ),
    # UDF Platform (Planning-Critical Extensions)
    "RustUdfPlatform": ("datafusion_engine.udf.platform", "RustUdfPlatform"),
    "RustUdfPlatformOptions": ("datafusion_engine.udf.platform", "RustUdfPlatformOptions"),
    "install_rust_udf_platform": ("datafusion_engine.udf.platform", "install_rust_udf_platform"),
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
