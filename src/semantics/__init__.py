"""Semantic pipeline compiler for CPG construction."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from semantics.column_types import (
        ColumnType,
        TableType,
        infer_column_type,
        infer_table_type,
    )
    from semantics.compiler import SemanticCompiler, TableInfo
    from semantics.config import (
        SemanticConfig,
        SemanticConfigSpec,
        semantic_config_from_spec,
    )
    from semantics.join_helpers import (
        join_by_path,
        join_by_span_contains,
        join_by_span_overlap,
    )
    from semantics.joins import (
        JoinInferenceError,
        JoinStrategy,
        JoinStrategyType,
        infer_join_strategy,
    )
    from semantics.pipeline_build import CpgBuildOptions, build_cpg, build_cpg_from_inferred_deps
    from semantics.schema import SemanticSchema
    from semantics.scip_normalize import scip_to_byte_offsets

__all__ = [
    "ColumnType",
    "CpgBuildOptions",
    "JoinInferenceError",
    "JoinStrategy",
    "JoinStrategyType",
    "SemanticCompiler",
    "SemanticConfig",
    "SemanticConfigSpec",
    "SemanticSchema",
    "TableInfo",
    "TableType",
    "build_cpg",
    "build_cpg_from_inferred_deps",
    "infer_column_type",
    "infer_join_strategy",
    "infer_table_type",
    "join_by_path",
    "join_by_span_contains",
    "join_by_span_overlap",
    "scip_to_byte_offsets",
    "semantic_config_from_spec",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ColumnType": ("semantics.column_types", "ColumnType"),
    "CpgBuildOptions": ("semantics.pipeline_build", "CpgBuildOptions"),
    "JoinInferenceError": ("semantics.joins", "JoinInferenceError"),
    "JoinStrategy": ("semantics.joins", "JoinStrategy"),
    "JoinStrategyType": ("semantics.joins", "JoinStrategyType"),
    "SemanticCompiler": ("semantics.compiler", "SemanticCompiler"),
    "SemanticConfig": ("semantics.config", "SemanticConfig"),
    "SemanticConfigSpec": ("semantics.config", "SemanticConfigSpec"),
    "SemanticSchema": ("semantics.schema", "SemanticSchema"),
    "TableInfo": ("semantics.compiler", "TableInfo"),
    "TableType": ("semantics.column_types", "TableType"),
    "build_cpg": ("semantics.pipeline_build", "build_cpg"),
    "build_cpg_from_inferred_deps": ("semantics.pipeline_build", "build_cpg_from_inferred_deps"),
    "infer_column_type": ("semantics.column_types", "infer_column_type"),
    "infer_join_strategy": ("semantics.joins", "infer_join_strategy"),
    "infer_table_type": ("semantics.column_types", "infer_table_type"),
    "join_by_path": ("semantics.join_helpers", "join_by_path"),
    "join_by_span_contains": ("semantics.join_helpers", "join_by_span_contains"),
    "join_by_span_overlap": ("semantics.join_helpers", "join_by_span_overlap"),
    "scip_to_byte_offsets": ("semantics.scip_normalize", "scip_to_byte_offsets"),
    "semantic_config_from_spec": ("semantics.config", "semantic_config_from_spec"),
}


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr_name = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
