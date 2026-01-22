"""DataFusion execution helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.bridge import (
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
    from datafusion_engine.df_builder import df_from_sqlglot, register_dataset
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
    from datafusion_engine.schema_introspection import SchemaIntrospector
    from datafusion_engine.schema_registry import (
        has_schema,
        nested_base_sql,
        nested_dataset_names,
        nested_schema_for,
        nested_schema_names,
        register_all_schemas,
        register_schema,
        schema_for,
        schema_names,
        schema_registry,
    )

__all__ = [
    "DEFAULT_DF_POLICY",
    "SCHEMA_HARDENING_PRESETS",
    "AdapterExecutionPolicy",
    "DataFusionCompileOptions",
    "DataFusionConfigPolicy",
    "DataFusionRuntimeProfile",
    "DataFusionSqlPolicy",
    "ExecutionLabel",
    "MemoryPool",
    "RegistryCatalogProvider",
    "RegistrySchemaProvider",
    "RegistryTarget",
    "SchemaHardeningProfile",
    "SchemaIntrospector",
    "apply_execution_label",
    "apply_execution_policy",
    "datafusion_to_table",
    "df_from_sqlglot",
    "has_schema",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "nested_base_sql",
    "nested_dataset_names",
    "nested_schema_for",
    "nested_schema_names",
    "register_all_schemas",
    "register_dataset",
    "register_dataset_df",
    "register_registry_catalog",
    "register_registry_delta_tables",
    "register_registry_exports",
    "register_schema",
    "register_view_specs",
    "registry_delta_table_paths",
    "registry_output_dir",
    "replay_substrait_bytes",
    "schema_for",
    "schema_names",
    "schema_registry",
    "snapshot_plans",
    "sqlglot_to_datafusion",
]

_EXPORTS: dict[str, tuple[str, str]] = {
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
    "datafusion_to_table": ("datafusion_engine.bridge", "datafusion_to_table"),
    "df_from_sqlglot": ("datafusion_engine.df_builder", "df_from_sqlglot"),
    "ibis_plan_to_datafusion": ("datafusion_engine.bridge", "ibis_plan_to_datafusion"),
    "ibis_plan_to_table": ("datafusion_engine.bridge", "ibis_plan_to_table"),
    "ibis_to_datafusion": ("datafusion_engine.bridge", "ibis_to_datafusion"),
    "replay_substrait_bytes": ("datafusion_engine.bridge", "replay_substrait_bytes"),
    "RegistryCatalogProvider": ("datafusion_engine.catalog_provider", "RegistryCatalogProvider"),
    "RegistrySchemaProvider": ("datafusion_engine.catalog_provider", "RegistrySchemaProvider"),
    "register_registry_catalog": (
        "datafusion_engine.catalog_provider",
        "register_registry_catalog",
    ),
    "has_schema": ("datafusion_engine.schema_registry", "has_schema"),
    "register_dataset": ("datafusion_engine.df_builder", "register_dataset"),
    "register_dataset_df": ("datafusion_engine.registry_bridge", "register_dataset_df"),
    "register_schema": ("datafusion_engine.schema_registry", "register_schema"),
    "register_all_schemas": ("datafusion_engine.schema_registry", "register_all_schemas"),
    "nested_base_sql": ("datafusion_engine.schema_registry", "nested_base_sql"),
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
    "snapshot_plans": ("datafusion_engine.runtime", "snapshot_plans"),
    "sqlglot_to_datafusion": ("datafusion_engine.bridge", "sqlglot_to_datafusion"),
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
