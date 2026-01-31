"""Statistics collection and estimation for semantic views.

This module provides utilities for collecting and estimating statistics on
semantic views, including row counts, column statistics, and size estimates.
Statistics can be used for query planning insights and performance analysis.

Usage
-----
>>> from datafusion import SessionContext
>>> from semantics.stats import ViewStats, collect_view_stats, estimate_row_count
>>>
>>> ctx = SessionContext()
>>> df = ctx.sql("SELECT * FROM my_table")
>>>
>>> # Quick row count estimation (no execution)
>>> estimated = estimate_row_count(df)
>>>
>>> # Full stats collection
>>> stats = collect_view_stats(df, view_name="my_view")
>>> print(stats.row_count)
"""

from __future__ import annotations

from semantics.stats.collector import (
    ColumnStats,
    ViewStats,
    ViewStatsCache,
    collect_view_stats,
    estimate_row_count,
)

__all__ = [
    "ColumnStats",
    "ViewStats",
    "ViewStatsCache",
    "collect_view_stats",
    "estimate_row_count",
]
