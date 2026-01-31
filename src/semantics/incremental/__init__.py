"""Incremental processing for semantic pipeline via CDF.

This module provides a change-data-feed pathway for incremental relationship
recomputation in the semantic CPG pipeline. It integrates with the existing
incremental infrastructure in ``src/incremental/`` while providing semantics-
specific join strategies.

The framework supports:
- CDF-aware join specifications for incremental relationship updates
- Multiple merge strategies (append, upsert, replace, delete-insert)
- Automatic detection of CDF-enabled DataFrames
- Integration with the existing ``CdfFilterPolicy`` infrastructure
"""

from __future__ import annotations

from semantics.incremental.cdf_joins import (
    DEFAULT_CDF_COLUMN,
    CDFJoinSpec,
    CDFMergeStrategy,
    build_incremental_join,
    incremental_join_enabled,
    is_cdf_enabled,
)

__all__ = [
    "DEFAULT_CDF_COLUMN",
    "CDFJoinSpec",
    "CDFMergeStrategy",
    "build_incremental_join",
    "incremental_join_enabled",
    "is_cdf_enabled",
]
