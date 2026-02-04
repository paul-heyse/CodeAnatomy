"""Incremental processing for semantic pipeline via CDF.

This module provides a change-data-feed pathway for incremental relationship
recomputation in the semantic CPG pipeline. It provides semantics-specific
join strategies and cursor-managed CDF reads.

The framework supports:
- CDF-aware join specifications for incremental relationship updates
- Multiple merge strategies (append, upsert, replace, delete-insert)
- Automatic detection of CDF-enabled DataFrames
- CDF filter policies for change-type selection
- Incremental configuration with cursor state management
- Cursor-based version tracking for incremental reads
- CDF reader for Delta table change data feeds
"""

from __future__ import annotations

from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_joins import (
    DEFAULT_CDF_COLUMN,
    CDFJoinSpec,
    CDFMergeStrategy,
    apply_cdf_merge,
    build_incremental_join,
    incremental_join_enabled,
    is_cdf_enabled,
    merge_incremental_results,
)
from semantics.incremental.cdf_reader import CdfReadOptions, CdfReadResult, read_cdf_changes
from semantics.incremental.config import SemanticIncrementalConfig

__all__ = [
    "DEFAULT_CDF_COLUMN",
    "CDFJoinSpec",
    "CDFMergeStrategy",
    "CdfCursor",
    "CdfCursorStore",
    "CdfReadOptions",
    "CdfReadResult",
    "SemanticIncrementalConfig",
    "apply_cdf_merge",
    "build_incremental_join",
    "incremental_join_enabled",
    "is_cdf_enabled",
    "merge_incremental_results",
    "read_cdf_changes",
]
