"""Join strategy inference and execution for semantic operations.

This module provides programmatic join inference based on schema annotations,
replacing hardcoded join logic with schema-driven strategies.

Join Strategy Types
-------------------
- EQUI_JOIN: Simple key equality (file_id = file_id)
- SPAN_OVERLAP: Spans overlap in same file
- SPAN_CONTAINS: One span contains another
- FOREIGN_KEY: FK relationship (entity_id -> def_id)
- SYMBOL_MATCH: Symbol name matching

Example:
-------
>>> from semantics.joins import infer_join_strategy
>>> from semantics.types import AnnotatedSchema
>>> import pyarrow as pa
>>>
>>> left_schema = pa.schema(
...     [
...         ("file_id", pa.string()),
...         ("bstart", pa.int64()),
...         ("bend", pa.int64()),
...     ]
... )
>>> right_schema = pa.schema(
...     [
...         ("file_id", pa.string()),
...         ("bstart", pa.int64()),
...         ("bend", pa.int64()),
...     ]
... )
>>> left = AnnotatedSchema.from_arrow_schema(left_schema)
>>> right = AnnotatedSchema.from_arrow_schema(right_schema)
>>> strategy = infer_join_strategy(left, right)
>>> strategy.strategy_type
<JoinStrategyType.SPAN_OVERLAP: 'span_overlap'>
"""

from __future__ import annotations

from semantics.joins.inference import (
    CONFIDENCE_BY_STRATEGY,
    JoinInferenceError,
    JoinStrategyResult,
    build_join_inference_confidence,
    infer_join_strategy,
    infer_join_strategy_with_confidence,
    require_join_strategy,
)
from semantics.joins.strategies import (
    FILE_EQUI_JOIN,
    SPAN_CONTAINS_STRATEGY,
    SPAN_OVERLAP_STRATEGY,
    JoinStrategy,
    JoinStrategyType,
)

__all__ = [
    "CONFIDENCE_BY_STRATEGY",
    "FILE_EQUI_JOIN",
    "SPAN_CONTAINS_STRATEGY",
    "SPAN_OVERLAP_STRATEGY",
    "JoinInferenceError",
    "JoinStrategy",
    "JoinStrategyResult",
    "JoinStrategyType",
    "build_join_inference_confidence",
    "infer_join_strategy",
    "infer_join_strategy_with_confidence",
    "require_join_strategy",
]
