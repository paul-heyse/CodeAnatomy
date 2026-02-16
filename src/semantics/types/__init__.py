"""Semantic type system for join inference and validation.

This module provides semantic typing for CPG columns:
- SemanticType: Role a column plays (FILE_ID, SPAN_START, etc.)
- CompatibilityGroup: Sets of columns that can be joined
- AnnotatedSchema: Schema with semantic annotations

Also re-exports ``column_types`` compatibility helpers:
- ColumnType, TableType, infer_column_type, infer_table_type

Example:
-------
>>> import pyarrow as pa
>>> from semantics.types import AnnotatedSchema, SemanticType
>>>
>>> schema = pa.schema(
...     [
...         ("file_id", pa.string()),
...         ("bstart", pa.int64()),
...         ("bend", pa.int64()),
...     ]
... )
>>> annotated = AnnotatedSchema.from_arrow_schema(schema)
>>> annotated.has_semantic_type(SemanticType.FILE_ID)
True
>>> annotated.join_key_columns()
(AnnotatedColumn(name='file_id', ...),)
"""

from semantics.column_types import (
    ColumnType,
    TableType,
    infer_column_type,
    infer_table_type,
)
from semantics.types.annotated_schema import AnnotatedColumn, AnnotatedSchema
from semantics.types.core import (
    STANDARD_COLUMNS,
    CompatibilityGroup,
    SemanticColumnSpec,
    SemanticType,
    columns_are_joinable,
    get_compatibility_groups,
    infer_semantic_type,
)

__all__ = [
    "STANDARD_COLUMNS",
    # New semantic type system
    "AnnotatedColumn",
    "AnnotatedSchema",
    # Backward compatibility re-exports from column_types
    "ColumnType",
    "CompatibilityGroup",
    "SemanticColumnSpec",
    "SemanticType",
    "TableType",
    "columns_are_joinable",
    "get_compatibility_groups",
    "infer_column_type",
    "infer_semantic_type",
    "infer_table_type",
]
