"""Statistics collector for semantic views.

This module provides dataclasses and functions for collecting statistics about
semantic views, including row counts, column-level statistics, and size estimates.
Statistics can be estimated without execution or computed exactly via execution.

The collector supports two modes:
1. Estimation mode: Uses DataFusion plan statistics when available
2. Execution mode: Runs the DataFrame to get exact counts

Usage
-----
>>> from semantics.stats import collect_view_stats, estimate_row_count
>>>
>>> # Estimate row count without execution
>>> estimated = estimate_row_count(df)
>>>
>>> # Collect full stats with optional exact counts
>>> stats = collect_view_stats(df, view_name="my_view", execute_for_exact=True)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

logger: Final = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnStats:
    """Statistics for a single column.

    Captures statistical information about a column in a semantic view,
    including null counts, distinct value estimates, and value ranges.
    All statistics are optional since they may not be available without
    execution.

    Attributes:
    ----------
    name
        Column name.
    null_count
        Number of null values (estimated or exact).
    distinct_count
        Number of distinct values (estimated).
    min_value
        Minimum value (if applicable), stored as string for serialization.
    max_value
        Maximum value (if applicable), stored as string for serialization.

    Examples:
    --------
    >>> col_stats = ColumnStats(
    ...     name="file_path",
    ...     null_count=0,
    ...     distinct_count=42,
    ... )
    >>> col_stats.name
    'file_path'
    """

    name: str
    null_count: int | None = None
    distinct_count: int | None = None
    min_value: str | None = None
    max_value: str | None = None

    def as_dict(self) -> dict[str, object]:
        """Convert to dictionary for serialization.

        Returns:
        -------
        dict[str, object]
            Dictionary representation of column statistics.
        """
        return {
            "name": self.name,
            "null_count": self.null_count,
            "distinct_count": self.distinct_count,
            "min_value": self.min_value,
            "max_value": self.max_value,
        }


@dataclass
class ViewStats:
    """Aggregated statistics for a semantic view.

    Collects statistics about a semantic view DataFrame including row counts,
    column statistics, and size estimates. Statistics can be estimated from
    plan metadata or computed exactly via execution.

    Attributes:
    ----------
    view_name
        Name of the semantic view.
    row_count
        Estimated or exact row count.
    row_count_exact
        True if row_count is exact (from execution), False if estimated.
    column_stats
        Per-column statistics keyed by column name.
    size_bytes
        Estimated size in bytes (if available).
    schema_fingerprint
        Optional schema fingerprint for cache invalidation.

    Examples:
    --------
    >>> stats = ViewStats(view_name="cst_refs_norm_v1")
    >>> stats.row_count = 1000
    >>> stats.row_count_exact = True
    >>> stats.add_column_stats(ColumnStats(name="file_path"))
    >>> len(stats.column_stats)
    1
    """

    view_name: str
    row_count: int | None = None
    row_count_exact: bool = False
    column_stats: dict[str, ColumnStats] = field(default_factory=dict)
    size_bytes: int | None = None
    schema_fingerprint: str | None = None

    def add_column_stats(self, stats: ColumnStats) -> None:
        """Add column statistics to the view stats.

        Parameters
        ----------
        stats
            Statistics for a single column.
        """
        self.column_stats[stats.name] = stats

    def column_names(self) -> tuple[str, ...]:
        """Return column names in the view.

        Returns:
        -------
        tuple[str, ...]
            Column names from collected statistics.
        """
        return tuple(self.column_stats.keys())

    def has_exact_count(self) -> bool:
        """Check if row count is exact.

        Returns:
        -------
        bool
            True if row_count is from execution, False if estimated.
        """
        return self.row_count_exact and self.row_count is not None

    def as_dict(self) -> dict[str, object]:
        """Convert to dictionary for serialization.

        Returns:
        -------
        dict[str, object]
            Dictionary representation of view statistics.
        """
        return {
            "view_name": self.view_name,
            "row_count": self.row_count,
            "row_count_exact": self.row_count_exact,
            "column_stats": {name: col.as_dict() for name, col in self.column_stats.items()},
            "size_bytes": self.size_bytes,
            "schema_fingerprint": self.schema_fingerprint,
        }


class ViewStatsCache:
    """Cache for view statistics.

    Provides a simple in-memory cache for view statistics, keyed by view name.
    Statistics can be invalidated by schema fingerprint changes or manually
    cleared.

    Attributes:
    ----------
    _cache : dict[str, ViewStats]
        Internal cache storage.

    Examples:
    --------
    >>> cache = ViewStatsCache()
    >>> stats = ViewStats(view_name="my_view", row_count=100)
    >>> cache.put(stats)
    >>> cached = cache.get("my_view")
    >>> cached.row_count
    100
    """

    def __init__(self) -> None:
        """Initialize an empty statistics cache."""
        self._cache: dict[str, ViewStats] = {}

    def get(self, view_name: str) -> ViewStats | None:
        """Retrieve cached statistics for a view.

        Parameters
        ----------
        view_name
            Name of the view to look up.

        Returns:
        -------
        ViewStats | None
            Cached statistics or None if not found.
        """
        return self._cache.get(view_name)

    def put(self, stats: ViewStats) -> None:
        """Store statistics in the cache.

        Parameters
        ----------
        stats
            Statistics to cache.
        """
        self._cache[stats.view_name] = stats

    def invalidate(self, view_name: str) -> None:
        """Remove statistics for a view from the cache.

        Parameters
        ----------
        view_name
            Name of the view to invalidate.
        """
        self._cache.pop(view_name, None)

    def clear(self) -> None:
        """Clear all cached statistics."""
        self._cache.clear()

    def __contains__(self, view_name: str) -> bool:
        """Check if statistics are cached for a view.

        Parameters
        ----------
        view_name
            Name of the view to check.

        Returns:
        -------
        bool
            True if statistics are cached.
        """
        return view_name in self._cache

    def __len__(self) -> int:
        """Return number of cached entries.

        Returns:
        -------
        int
            Number of views with cached statistics.
        """
        return len(self._cache)


def _parse_row_count_from_line(line: str) -> int | None:
    """Extract row count from a plan display line containing 'rows='.

    Parameters
    ----------
    line
        A single line from plan display output.

    Returns:
    -------
    int | None
        Extracted row count or None if parsing fails.
    """
    parts = line.split("rows=")
    if len(parts) <= 1:
        return None
    num_part = parts[1].split()[0].strip(",)")
    if num_part.isdigit():
        return int(num_part)
    return None


def estimate_row_count(df: DataFrame) -> int | None:
    """Estimate row count from DataFrame without full execution.

    Attempts to extract row count from DataFusion plan statistics when
    available. This is a best-effort estimation that does not execute
    the DataFrame.

    Parameters
    ----------
    df
        DataFrame to estimate row count for.

    Returns:
    -------
    int | None
        Estimated row count, or None if statistics are not available.

    Notes:
    -----
    DataFusion maintains statistics for table scans and some operations.
    For complex plans with joins or aggregations, statistics may not
    propagate and this function will return None.

    Examples:
    --------
    >>> # Table scan may have statistics from parquet metadata
    >>> df = ctx.table("my_table")
    >>> estimate_row_count(df)
    1000
    >>>
    >>> # Complex plan may not have statistics
    >>> df_joined = df1.join(df2, on="key")
    >>> estimate_row_count(df_joined)
    None
    """
    try:
        plan = df.logical_plan()
        plan_str = plan.display()
    except (AttributeError, RuntimeError) as exc:
        logger.debug("Could not get logical plan: %s", exc)
        return None

    if "rows=" not in plan_str:
        return None

    for line in plan_str.split("\n"):
        if "rows=" not in line:
            continue
        row_count = _parse_row_count_from_line(line)
        if row_count is not None:
            return row_count

    return None


def _collect_column_stats_from_schema(df: DataFrame) -> dict[str, ColumnStats]:
    """Collect basic column stats from DataFrame schema.

    Parameters
    ----------
    df
        DataFrame to collect schema from.

    Returns:
    -------
    dict[str, ColumnStats]
        Column statistics keyed by column name.
    """
    result: dict[str, ColumnStats] = {}
    try:
        schema = df.schema()
    except (AttributeError, RuntimeError) as exc:
        logger.debug("Could not collect schema info: %s", exc)
        return result

    # PyArrow schema iteration
    for i in range(len(schema)):
        pa_field = schema.field(i)
        col_stats = ColumnStats(name=pa_field.name)
        result[pa_field.name] = col_stats

    return result


def collect_view_stats(
    df: DataFrame,
    *,
    view_name: str,
    execute_for_exact: bool = False,
    ctx: SessionContext | None = None,
    schema_fingerprint: str | None = None,
) -> ViewStats:
    """Collect statistics for a semantic view DataFrame.

    Gathers statistics about a DataFrame including row count estimates,
    column information from the schema, and optional exact counts via
    execution.

    Parameters
    ----------
    df
        DataFrame to collect statistics from.
    view_name
        Name of the semantic view (for identification).
    execute_for_exact
        If True, execute the DataFrame to get exact row counts.
        This may be expensive for large DataFrames.
    ctx
        Session context (optional, reserved for future query-based stats).
    schema_fingerprint
        Optional schema fingerprint for cache invalidation.

    Returns:
    -------
    ViewStats
        Collected statistics (estimated unless execute_for_exact=True).

    Examples:
    --------
    >>> # Quick stats without execution
    >>> stats = collect_view_stats(df, view_name="cst_refs_norm_v1")
    >>> stats.row_count_exact
    False
    >>>
    >>> # Exact stats with execution
    >>> stats = collect_view_stats(
    ...     df,
    ...     view_name="cst_refs_norm_v1",
    ...     execute_for_exact=True,
    ... )
    >>> stats.row_count_exact
    True
    """
    # Acknowledge ctx for future extension
    _ = ctx

    stats = ViewStats(
        view_name=view_name,
        schema_fingerprint=schema_fingerprint,
    )

    # Try to estimate row count from plan
    estimated = estimate_row_count(df)
    if estimated is not None:
        stats.row_count = estimated
        stats.row_count_exact = False

    # Execute for exact count if requested
    if execute_for_exact:
        try:
            count_result = df.count()
            stats.row_count = count_result
            stats.row_count_exact = True
        except (RuntimeError, AttributeError) as exc:
            logger.warning("Could not execute DataFrame for exact count: %s", exc)

    # Collect column stats from schema
    stats.column_stats = _collect_column_stats_from_schema(df)

    return stats


__all__ = [
    "ColumnStats",
    "ViewStats",
    "ViewStatsCache",
    "collect_view_stats",
    "estimate_row_count",
]
