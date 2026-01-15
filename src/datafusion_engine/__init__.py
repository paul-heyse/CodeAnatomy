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
    from datafusion_engine.compile_options import (
        DataFusionCompileOptions,
        DataFusionFallbackEvent,
        DataFusionSqlPolicy,
    )
    from datafusion_engine.df_builder import df_from_sqlglot, register_dataset
    from datafusion_engine.param_tables import (
        ensure_param_schema,
        register_param_arrow_table,
        register_param_tables_df,
    )
    from datafusion_engine.registry_bridge import register_dataset_df
    from datafusion_engine.runtime import (
        DEFAULT_DF_POLICY,
        AdapterExecutionPolicy,
        DataFusionConfigPolicy,
        DataFusionRuntimeProfile,
        ExecutionLabel,
        MemoryPool,
        apply_execution_label,
        apply_execution_policy,
        snapshot_plans,
    )

__all__ = [
    "DEFAULT_DF_POLICY",
    "AdapterExecutionPolicy",
    "DataFusionCompileOptions",
    "DataFusionConfigPolicy",
    "DataFusionFallbackEvent",
    "DataFusionRuntimeProfile",
    "DataFusionSqlPolicy",
    "ExecutionLabel",
    "MemoryPool",
    "apply_execution_label",
    "apply_execution_policy",
    "datafusion_to_table",
    "df_from_sqlglot",
    "ensure_param_schema",
    "ibis_plan_to_datafusion",
    "ibis_plan_to_table",
    "ibis_to_datafusion",
    "register_dataset",
    "register_dataset_df",
    "register_param_arrow_table",
    "register_param_tables_df",
    "replay_substrait_bytes",
    "snapshot_plans",
    "sqlglot_to_datafusion",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "DEFAULT_DF_POLICY": ("datafusion_engine.runtime", "DEFAULT_DF_POLICY"),
    "AdapterExecutionPolicy": ("datafusion_engine.runtime", "AdapterExecutionPolicy"),
    "ExecutionLabel": ("datafusion_engine.runtime", "ExecutionLabel"),
    "DataFusionCompileOptions": ("datafusion_engine.compile_options", "DataFusionCompileOptions"),
    "DataFusionFallbackEvent": ("datafusion_engine.compile_options", "DataFusionFallbackEvent"),
    "DataFusionConfigPolicy": ("datafusion_engine.runtime", "DataFusionConfigPolicy"),
    "DataFusionRuntimeProfile": ("datafusion_engine.runtime", "DataFusionRuntimeProfile"),
    "DataFusionSqlPolicy": ("datafusion_engine.compile_options", "DataFusionSqlPolicy"),
    "MemoryPool": ("datafusion_engine.runtime", "MemoryPool"),
    "apply_execution_label": ("datafusion_engine.runtime", "apply_execution_label"),
    "apply_execution_policy": ("datafusion_engine.runtime", "apply_execution_policy"),
    "datafusion_to_table": ("datafusion_engine.bridge", "datafusion_to_table"),
    "df_from_sqlglot": ("datafusion_engine.df_builder", "df_from_sqlglot"),
    "ensure_param_schema": ("datafusion_engine.param_tables", "ensure_param_schema"),
    "ibis_plan_to_datafusion": ("datafusion_engine.bridge", "ibis_plan_to_datafusion"),
    "ibis_plan_to_table": ("datafusion_engine.bridge", "ibis_plan_to_table"),
    "ibis_to_datafusion": ("datafusion_engine.bridge", "ibis_to_datafusion"),
    "replay_substrait_bytes": ("datafusion_engine.bridge", "replay_substrait_bytes"),
    "register_dataset": ("datafusion_engine.df_builder", "register_dataset"),
    "register_dataset_df": ("datafusion_engine.registry_bridge", "register_dataset_df"),
    "register_param_arrow_table": ("datafusion_engine.param_tables", "register_param_arrow_table"),
    "register_param_tables_df": ("datafusion_engine.param_tables", "register_param_tables_df"),
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
