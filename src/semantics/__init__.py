"""Semantic pipeline compiler for CPG construction.

This module provides a minimal semantic layer that transforms extraction tables
into CPG outputs using 10 composable rules. The core insight: column presence
determines available operations.

Usage
-----
>>> from datafusion_engine.session.runtime import DataFusionRuntimeProfile
>>> from semantics.pipeline import build_cpg
>>>
>>> profile = DataFusionRuntimeProfile()
>>> ctx = profile.session_context()
>>> # ... register extraction tables ...
>>> build_cpg(ctx, runtime_profile=profile)
>>> # Outputs are registered with canonical names (e.g., cpg_nodes_v1, cpg_edges_v1).
"""

from __future__ import annotations

from semantics.column_types import ColumnType, TableType, infer_column_type, infer_table_type
from semantics.compiler import SemanticCompiler, TableInfo
from semantics.config import SemanticConfig
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
from semantics.schema import SemanticSchema
from semantics.scip_normalize import scip_to_byte_offsets

__all__ = [
    "ColumnType",
    "JoinInferenceError",
    "JoinStrategy",
    "JoinStrategyType",
    "SemanticCompiler",
    "SemanticConfig",
    "SemanticSchema",
    "TableInfo",
    "TableType",
    "infer_column_type",
    "infer_join_strategy",
    "infer_table_type",
    "join_by_path",
    "join_by_span_contains",
    "join_by_span_overlap",
    "scip_to_byte_offsets",
]
