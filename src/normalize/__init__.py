"""Normalization helpers for extracted tables."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.normalize_ids import (
        DEF_USE_EVENT_ID_SPEC,
        DIAG_ID_SPEC,
        REACH_EDGE_ID_SPEC,
        TYPE_EXPR_ID_SPEC,
        TYPE_ID_SPEC,
        hash_spec,
    )
    from normalize.dataset_builders import field, field_name, fields
    from normalize.dataset_specs import dataset_contract_schema
    from normalize.df_view_builders import (
        VIEW_BUILDERS,
        VIEW_BUNDLE_BUILDERS,
        cfg_blocks_df_builder,
        cfg_edges_df_builder,
        def_use_events_df_builder,
        diagnostics_df_builder,
        reaching_defs_df_builder,
        span_errors_df_builder,
        type_exprs_df_builder,
        type_nodes_df_builder,
    )
    from normalize.diagnostic_types import (
        DIAG_DETAIL_STRUCT,
        DIAG_DETAILS_TYPE,
        DIAG_TAGS_TYPE,
    )
    from normalize.registry_runtime import (
        dataset_alias,
        dataset_contract,
        dataset_input_columns,
        dataset_input_schema,
        dataset_metadata_spec,
        dataset_name_from_alias,
        dataset_names,
        dataset_schema,
        dataset_schema_policy,
        dataset_spec,
        dataset_specs,
        dataset_table_spec,
    )

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DEF_USE_EVENT_ID_SPEC": ("datafusion_engine.normalize_ids", "DEF_USE_EVENT_ID_SPEC"),
    "DIAG_DETAILS_TYPE": ("normalize.diagnostic_types", "DIAG_DETAILS_TYPE"),
    "DIAG_DETAIL_STRUCT": ("normalize.diagnostic_types", "DIAG_DETAIL_STRUCT"),
    "DIAG_ID_SPEC": ("datafusion_engine.normalize_ids", "DIAG_ID_SPEC"),
    "DIAG_TAGS_TYPE": ("normalize.diagnostic_types", "DIAG_TAGS_TYPE"),
    "REACH_EDGE_ID_SPEC": ("datafusion_engine.normalize_ids", "REACH_EDGE_ID_SPEC"),
    "TYPE_EXPR_ID_SPEC": ("datafusion_engine.normalize_ids", "TYPE_EXPR_ID_SPEC"),
    "TYPE_ID_SPEC": ("datafusion_engine.normalize_ids", "TYPE_ID_SPEC"),
    "VIEW_BUNDLE_BUILDERS": ("normalize.df_view_builders", "VIEW_BUNDLE_BUILDERS"),
    "VIEW_BUILDERS": ("normalize.df_view_builders", "VIEW_BUILDERS"),
    "cfg_blocks_df_builder": ("normalize.df_view_builders", "cfg_blocks_df_builder"),
    "cfg_edges_df_builder": ("normalize.df_view_builders", "cfg_edges_df_builder"),
    "def_use_events_df_builder": ("normalize.df_view_builders", "def_use_events_df_builder"),
    "diagnostics_df_builder": ("normalize.df_view_builders", "diagnostics_df_builder"),
    "reaching_defs_df_builder": ("normalize.df_view_builders", "reaching_defs_df_builder"),
    "span_errors_df_builder": ("normalize.df_view_builders", "span_errors_df_builder"),
    "type_exprs_df_builder": ("normalize.df_view_builders", "type_exprs_df_builder"),
    "type_nodes_df_builder": ("normalize.df_view_builders", "type_nodes_df_builder"),
    "dataset_alias": ("normalize.registry_runtime", "dataset_alias"),
    "dataset_contract": ("normalize.registry_runtime", "dataset_contract"),
    "dataset_contract_schema": ("normalize.dataset_specs", "dataset_contract_schema"),
    "dataset_input_columns": ("normalize.registry_runtime", "dataset_input_columns"),
    "dataset_input_schema": ("normalize.registry_runtime", "dataset_input_schema"),
    "dataset_metadata_spec": ("normalize.registry_runtime", "dataset_metadata_spec"),
    "dataset_name_from_alias": ("normalize.registry_runtime", "dataset_name_from_alias"),
    "dataset_names": ("normalize.registry_runtime", "dataset_names"),
    "dataset_schema": ("normalize.registry_runtime", "dataset_schema"),
    "dataset_schema_policy": ("normalize.registry_runtime", "dataset_schema_policy"),
    "dataset_spec": ("normalize.registry_runtime", "dataset_spec"),
    "dataset_specs": ("normalize.registry_runtime", "dataset_specs"),
    "dataset_table_spec": ("normalize.registry_runtime", "dataset_table_spec"),
    "field": ("normalize.dataset_builders", "field"),
    "field_name": ("normalize.dataset_builders", "field_name"),
    "fields": ("normalize.dataset_builders", "fields"),
    "hash_spec": ("datafusion_engine.normalize_ids", "hash_spec"),
}


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = [
    "DEF_USE_EVENT_ID_SPEC",
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_ID_SPEC",
    "DIAG_TAGS_TYPE",
    "REACH_EDGE_ID_SPEC",
    "TYPE_EXPR_ID_SPEC",
    "TYPE_ID_SPEC",
    "VIEW_BUILDERS",
    "VIEW_BUNDLE_BUILDERS",
    "cfg_blocks_df_builder",
    "cfg_edges_df_builder",
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_columns",
    "dataset_input_schema",
    "dataset_metadata_spec",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_schema",
    "dataset_schema_policy",
    "dataset_spec",
    "dataset_specs",
    "dataset_table_spec",
    "def_use_events_df_builder",
    "diagnostics_df_builder",
    "field",
    "field_name",
    "fields",
    "hash_spec",
    "reaching_defs_df_builder",
    "span_errors_df_builder",
    "type_exprs_df_builder",
    "type_nodes_df_builder",
]
