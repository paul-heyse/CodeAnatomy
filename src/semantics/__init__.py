"""Semantic pipeline compiler for CPG construction.

This module provides a minimal semantic layer that transforms extraction tables
into CPG outputs using 10 composable rules. The core insight: column presence
determines available operations.

Usage
-----
>>> from semantics import SemanticCompiler
>>> from datafusion import SessionContext
>>>
>>> ctx = SessionContext()
>>> # ... register extraction tables ...
>>> compiler = SemanticCompiler(ctx)
>>> ctx.register_view("cst_refs_norm", compiler.normalize("cst_refs", prefix="ref"))
>>> ctx.register_view(
...     "rel_name_symbol",
...     compiler.relate(
...         "cst_refs_norm",
...         "scip_occurrences",
...         join_type="overlap",
...         filter_sql="is_read = true",
...         origin="cst_ref",
...     ),
... )
"""

from __future__ import annotations

from semantics.compiler import SemanticCompiler, TableInfo
from semantics.config import SemanticConfig
from semantics.join_helpers import (
    join_by_path,
    join_by_span_contains,
    join_by_span_overlap,
)
from semantics.schema import SemanticSchema
from semantics.types import ColumnType, TableType, infer_column_type, infer_table_type

__all__ = [
    "ColumnType",
    "SemanticCompiler",
    "SemanticConfig",
    "SemanticSchema",
    "TableInfo",
    "TableType",
    "infer_column_type",
    "infer_table_type",
    "join_by_path",
    "join_by_span_contains",
    "join_by_span_overlap",
]
