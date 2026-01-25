"""DataFusion execution helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.bridge import (
        datafusion_read_table,
        datafusion_to_table,
        ibis_plan_to_datafusion,
        ibis_plan_to_table,
        ibis_to_datafusion,
        replay_substrait_bytes,
        sqlglot_to_datafusion,
    )
    from datafusion_engine.catalog_provider import (
        RegistryCatalogProvider,
        RegistrySchemaProvider,
        register_registry_catalog,
    )
    from datafusion_engine.compile_options import (
        DataFusionCompileOptions,
        DataFusionSqlPolicy,
    )

    # New modular architecture modules
    from datafusion_engine.compile_pipeline import (
        CompilationPipeline,
        CompiledExpression,
        CompileOptions,
    )
    from datafusion_engine.diagnostics import (
        DiagnosticsContext,
        DiagnosticsRecorder,
        DiagnosticsSink,
        InMemoryDiagnosticsSink,
    )
    from datafusion_engine.introspection import (
        IntrospectionCache,
        IntrospectionSnapshot,
    )
    from datafusion_engine.io_adapter import DataFusionIOAdapter
    from datafusion_engine.param_binding import (
        DataFusionParamBindings,
        apply_bindings_to_context,
        resolve_param_bindings,
    )
    from datafusion_engine.parameterized_execution import (
        ParameterizedRulepack,
        ParameterSpec,
        RulepackRegistry,
    )
    from datafusion_engine.registry_bridge import register_dataset_df
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
        has_schema,
        nested_base_df,
        nested_dataset_names,
        nested_schema_for,
        nested_schema_names,
        register_all_schemas,
        register_schema,
        schema_for,
        schema_names,
        schema_registry,
    )
    from datafusion_engine.semantic_diff import (
        ChangeCategory,
        RebuildPolicy,
        SemanticChange,
        SemanticDiff,
        compute_rebuild_needed,
    )
    from datafusion_engine.sql_policy_engine import (
        CompilationArtifacts,
        SQLPolicyProfile,
        compile_sql_policy,
        render_for_execution,
    )
    from datafusion_engine.sql_safety import (
        ExecutionContext,
        ExecutionPolicy,
        SafeExecutor,
        execute_with_policy,
        validate_sql_safety,
    )
    from datafusion_engine.streaming_executor import (
        StreamingExecutionResult,
        StreamingExecutor,
    )
    from datafusion_engine.write_pipeline import (
        ParquetWritePolicy,
        WriteFormat,
        WriteMode,
        WritePipeline,
        WriteRequest,
    )

__all__ = [
    # Runtime and configuration
    "DEFAULT_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    # New modular architecture - Semantic Diff
    "ChangeCategory",
    # New modular architecture - Schema Contracts
    "ColumnContract",
    "CompilationArtifacts",
    # New modular architecture - Compilation Pipeline
    "CompilationPipeline",
    "CompileOptions",
    "CompiledExpression",
    "ContractRegistry",
    "DataFusionCompileOptions",
    "DataFusionConfigPolicy",
    # New modular architecture - IO Adapter
    "DataFusionIOAdapter",
    # New modular architecture - Parameter Binding
    "DataFusionParamBindings",
    "DataFusionRuntimeProfile",
    "DataFusionSqlPolicy",
    # New modular architecture - Diagnostics
    "DiagnosticsContext",
    "DiagnosticsRecorder",
    "DiagnosticsSink",
    "EvolutionPolicy",
    # New modular architecture - SQL Safety
    "ExecutionContext",
    "ExecutionLabel",
    "ExecutionPolicy",
    "InMemoryDiagnosticsSink",
    # New modular architecture - Introspection
    "IntrospectionCache",
    "IntrospectionSnapshot",
    "MemoryPool",
    "ParameterSpec",
    # New modular architecture - Parameterized Execution
    "ParameterizedRulepack",
    # New modular architecture - Write Pipeline
    "ParquetWritePolicy",
    "RebuildPolicy",
    # Catalog and schema
    "RegistryCatalogProvider",
    "RegistrySchemaProvider",
    "RegistryTarget",
    "RulepackRegistry",
    "SQLPolicyProfile",
    "SafeExecutor",
    "SchemaContract",
    "SchemaHardeningProfile",
    "SchemaIntrospector",
    "SchemaViolation",
    "SchemaViolationType",
    "SemanticChange",
    "SemanticDiff",
    # New modular architecture - Streaming Execution
    "StreamingExecutionResult",
    "StreamingExecutor",
    "WriteFormat",
    "WriteMode",
    "WritePipeline",
    "WriteRequest",
    "apply_bindings_to_context",
    "apply_execution_label",
    "apply_execution_policy",
    "compile_sql_policy",
    "compute_rebuild_needed",
    # Bridge and execution
    "datafusion_read_table",
    "datafusion_to_table",
    "execute_with_policy",
    "has_schema",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "nested_base_df",
    "nested_dataset_names",
    "nested_schema_for",
    "nested_schema_names",
    "register_all_schemas",
    "register_dataset_df",
    "register_registry_catalog",
    "register_registry_delta_tables",
    "register_registry_exports",
    "register_schema",
    "register_view_specs",
    "registry_delta_table_paths",
    "registry_output_dir",
    "render_for_execution",
    "replay_substrait_bytes",
    "resolve_param_bindings",
    "schema_for",
    "schema_names",
    "schema_registry",
    "snapshot_plans",
    "sqlglot_to_datafusion",
    "validate_sql_safety",
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
    "has_schema": ("datafusion_engine.schema_registry", "has_schema"),
    "register_dataset_df": ("datafusion_engine.registry_bridge", "register_dataset_df"),
    "register_schema": ("datafusion_engine.schema_registry", "register_schema"),
    "register_all_schemas": ("datafusion_engine.schema_registry", "register_all_schemas"),
    "nested_base_df": ("datafusion_engine.schema_registry", "nested_base_df"),
    "nested_dataset_names": ("datafusion_engine.schema_registry", "nested_dataset_names"),
    "nested_schema_for": ("datafusion_engine.schema_registry", "nested_schema_for"),
    "nested_schema_names": ("datafusion_engine.schema_registry", "nested_schema_names"),
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
    "schema_for": ("datafusion_engine.schema_registry", "schema_for"),
    "schema_names": ("datafusion_engine.schema_registry", "schema_names"),
    "schema_registry": ("datafusion_engine.schema_registry", "schema_registry"),
    "SchemaIntrospector": ("datafusion_engine.schema_introspection", "SchemaIntrospector"),
    # Bridge and execution
    "datafusion_read_table": ("datafusion_engine.bridge", "datafusion_read_table"),
    "datafusion_to_table": ("datafusion_engine.bridge", "datafusion_to_table"),
    "ibis_plan_to_datafusion": ("datafusion_engine.bridge", "ibis_plan_to_datafusion"),
    "ibis_plan_to_table": ("datafusion_engine.bridge", "ibis_plan_to_table"),
    "ibis_to_datafusion": ("datafusion_engine.bridge", "ibis_to_datafusion"),
    "replay_substrait_bytes": ("datafusion_engine.bridge", "replay_substrait_bytes"),
    "sqlglot_to_datafusion": ("datafusion_engine.bridge", "sqlglot_to_datafusion"),
    # Compilation Pipeline
    "CompilationPipeline": ("datafusion_engine.compile_pipeline", "CompilationPipeline"),
    "CompiledExpression": ("datafusion_engine.compile_pipeline", "CompiledExpression"),
    "CompileOptions": ("datafusion_engine.compile_pipeline", "CompileOptions"),
    "CompilationArtifacts": ("datafusion_engine.sql_policy_engine", "CompilationArtifacts"),
    "SQLPolicyProfile": ("datafusion_engine.sql_policy_engine", "SQLPolicyProfile"),
    "compile_sql_policy": ("datafusion_engine.sql_policy_engine", "compile_sql_policy"),
    "render_for_execution": ("datafusion_engine.sql_policy_engine", "render_for_execution"),
    # Streaming Execution
    "StreamingExecutionResult": (
        "datafusion_engine.streaming_executor",
        "StreamingExecutionResult",
    ),
    "StreamingExecutor": ("datafusion_engine.streaming_executor", "StreamingExecutor"),
    # Parameter Binding
    "DataFusionParamBindings": ("datafusion_engine.param_binding", "DataFusionParamBindings"),
    "apply_bindings_to_context": (
        "datafusion_engine.param_binding",
        "apply_bindings_to_context",
    ),
    "resolve_param_bindings": ("datafusion_engine.param_binding", "resolve_param_bindings"),
    # Parameterized Execution
    "ParameterizedRulepack": (
        "datafusion_engine.parameterized_execution",
        "ParameterizedRulepack",
    ),
    "ParameterSpec": ("datafusion_engine.parameterized_execution", "ParameterSpec"),
    "RulepackRegistry": ("datafusion_engine.parameterized_execution", "RulepackRegistry"),
    # SQL Safety
    "ExecutionContext": ("datafusion_engine.sql_safety", "ExecutionContext"),
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
    # IO Adapter
    "DataFusionIOAdapter": ("datafusion_engine.io_adapter", "DataFusionIOAdapter"),
    # Write Pipeline
    "ParquetWritePolicy": ("datafusion_engine.write_pipeline", "ParquetWritePolicy"),
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
