"""File pruning policies and evaluation for Delta Lake tables."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from datafusion import SessionContext


@dataclass(frozen=True)
class FilePruningPolicy:
    """File pruning policy with partition and statistics filters.

    Attributes
    ----------
    partition_filters : list[str]
        SQL predicates for partition column filtering (e.g., ["year = '2024'", "month = '01'"]).
    stats_filters : list[str]
        SQL predicates for statistics-based filtering (e.g., ["id >= 100", "id <= 500"]).
    """

    partition_filters: list[str]
    stats_filters: list[str]

    def __post_init__(self) -> None:
        """Validate policy values.

        Raises
        ------
        TypeError
            Raised when partition or stats filters are not lists.
        """
        if not isinstance(self.partition_filters, list):
            msg = "partition_filters must be a list of SQL predicates"
            raise TypeError(msg)
        if not isinstance(self.stats_filters, list):
            msg = "stats_filters must be a list of SQL predicates"
            raise TypeError(msg)

    def has_filters(self) -> bool:
        """Check if any filters are defined.

        Returns
        -------
        bool
            True if partition or stats filters are present.
        """
        return bool(self.partition_filters) or bool(self.stats_filters)

    def to_sql_predicate(self) -> str | None:
        """Convert policy to a SQL WHERE clause predicate.

        Returns
        -------
        str | None
            Combined SQL predicate, or None if no filters are defined.
        """
        all_filters = list(self.partition_filters) + list(self.stats_filters)
        if not all_filters:
            return None
        return " AND ".join(f"({f})" for f in all_filters)


@dataclass(frozen=True)
class FilePruningResult:
    """Result of file pruning evaluation.

    Attributes
    ----------
    candidate_count : int
        Number of files that passed pruning filters.
    total_files : int
        Total number of files before pruning.
    pruned_count : int
        Number of files pruned (excluded).
    candidate_paths : list[str]
        List of file paths that passed filters.
    """

    candidate_count: int
    total_files: int
    pruned_count: int
    candidate_paths: list[str]

    @property
    def pruned_percentage(self) -> float:
        """Calculate the percentage of files pruned.

        Returns
        -------
        float
            Percentage of files pruned (0-100).
        """
        if self.total_files == 0:
            return 0.0
        return (self.pruned_count / self.total_files) * 100.0

    @property
    def retention_percentage(self) -> float:
        """Calculate the percentage of files retained.

        Returns
        -------
        float
            Percentage of files retained (0-100).
        """
        return 100.0 - self.pruned_percentage


def evaluate_filters_against_index(
    index: pa.Table,
    policy: FilePruningPolicy,
    ctx: SessionContext,
) -> pa.Table:
    """Evaluate pruning filters against the file index using DataFusion SQL.

    This function registers the file index as a temporary table in DataFusion,
    executes SQL predicates to filter files, and returns the filtered index.

    Parameters
    ----------
    index : pa.Table
        File index table from build_delta_file_index.
    policy : FilePruningPolicy
        Pruning policy with partition and statistics filters.
    ctx : SessionContext
        DataFusion session context for SQL evaluation.

    Returns
    -------
    pa.Table
        Filtered file index table with only candidate files.
    """
    if not policy.has_filters():
        return index

    # Convert policy to SQL predicate
    predicate = policy.to_sql_predicate()
    if predicate is None:
        return index

    # Register the index as a temporary table
    temp_table_name = f"__file_index_{uuid.uuid4().hex}"
    try:
        ctx.register_record_batches(temp_table_name, [list(index.to_batches())])

        # Construct and execute filter query
        sql = _build_filter_query(temp_table_name, policy)
        filtered_df = ctx.sql(sql)
        return filtered_df.to_arrow_table()
    finally:
        # Deregister temporary table
        _deregister_table(ctx, temp_table_name)


def select_candidate_files(
    index: pa.Table,
    policy: FilePruningPolicy,
) -> list[str]:
    """Select candidate file paths from the index using simple Python filtering.

    This is a lightweight alternative to evaluate_filters_against_index that
    doesn't require DataFusion for simple partition equality checks.

    Parameters
    ----------
    index : pa.Table
        File index table from build_delta_file_index.
    policy : FilePruningPolicy
        Pruning policy with partition and statistics filters.

    Returns
    -------
    list[str]
        List of file paths that pass the filters.
    """
    if not policy.has_filters():
        # No filters - return all paths
        path_col = index.column("path")
        return [str(p) for p in path_col.to_pylist()]

    # Parse partition filters for simple equality checks
    partition_constraints = _parse_partition_filters(policy.partition_filters)

    # Filter files based on partition values
    candidate_paths: list[str] = []
    path_col = index.column("path")
    partition_values_col = index.column("partition_values")

    for i in range(index.num_rows):
        path = str(path_col[i].as_py())
        partition_values: dict[str, Any] | None = partition_values_col[i].as_py()

        if partition_values is None:
            partition_values = {}

        # Check partition constraints
        if _matches_partition_constraints(partition_values, partition_constraints):
            candidate_paths.append(path)

    return candidate_paths


def evaluate_and_select_files(
    index: pa.Table,
    policy: FilePruningPolicy,
    ctx: SessionContext | None = None,
) -> FilePruningResult:
    """Evaluate filters and return pruning result with statistics.

    Parameters
    ----------
    index : pa.Table
        File index table from build_delta_file_index.
    policy : FilePruningPolicy
        Pruning policy with partition and statistics filters.
    ctx : SessionContext | None
        Optional DataFusion session context for SQL-based filtering.
        If None, uses simple Python filtering.

    Returns
    -------
    FilePruningResult
        Pruning result with candidate files and statistics.
    """
    total_files = index.num_rows

    if ctx is not None and policy.stats_filters:
        # Use DataFusion for complex stats filtering
        filtered_index = evaluate_filters_against_index(index, policy, ctx)
        candidate_paths = [str(p) for p in filtered_index.column("path").to_pylist()]
    else:
        # Use simple Python filtering
        candidate_paths = select_candidate_files(index, policy)

    candidate_count = len(candidate_paths)
    pruned_count = total_files - candidate_count

    return FilePruningResult(
        candidate_count=candidate_count,
        total_files=total_files,
        pruned_count=pruned_count,
        candidate_paths=candidate_paths,
    )


def _build_filter_query(table_name: str, policy: FilePruningPolicy) -> str:
    """Build SQL filter query for file index.

    Parameters
    ----------
    table_name : str
        Name of the registered file index table.
    policy : FilePruningPolicy
        Pruning policy with filters.

    Returns
    -------
    str
        SQL query string.
    """
    predicate = policy.to_sql_predicate()
    if predicate is None:
        return f"SELECT * FROM {table_name}"

    # For partition filters, we need to extract from the map
    # For now, use a simple approach that works with DataFusion
    partition_predicates = _build_partition_predicates(policy.partition_filters)

    if partition_predicates and policy.stats_filters:
        combined = (
            partition_predicates + " AND " + " AND ".join(f"({f})" for f in policy.stats_filters)
        )
    elif partition_predicates:
        combined = partition_predicates
    elif policy.stats_filters:
        combined = " AND ".join(f"({f})" for f in policy.stats_filters)
    else:
        combined = "TRUE"

    return f"SELECT * FROM {table_name} WHERE {combined}"


def _build_partition_predicates(partition_filters: list[str]) -> str:
    """Build partition filter predicates.

    For now, this is a simplified implementation that assumes
    partition filters are already in a compatible SQL format.

    Parameters
    ----------
    partition_filters : list[str]
        List of partition filter predicates.

    Returns
    -------
    str
        Combined partition predicates.
    """
    if not partition_filters:
        return ""
    return " AND ".join(f"({f})" for f in partition_filters)


def _parse_partition_filters(filters: list[str]) -> dict[str, str]:
    """Parse simple partition equality filters into a constraint map.

    Parameters
    ----------
    filters : list[str]
        List of partition filter predicates (e.g., ["year = '2024'", "month = '01'"]).

    Returns
    -------
    dict[str, str]
        Map of partition column names to required values.
    """
    constraints: dict[str, str] = {}
    expected_parts = 2
    for filter_expr in filters:
        # Simple parser for "column = 'value'" format
        parts = filter_expr.split("=")
        if len(parts) == expected_parts:
            col = parts[0].strip()
            val = parts[1].strip().strip("'\"")
            constraints[col] = val
    return constraints


def _matches_partition_constraints(
    partition_values: dict[str, Any],
    constraints: dict[str, str],
) -> bool:
    """Check if partition values match the constraints.

    Parameters
    ----------
    partition_values : dict[str, Any]
        Partition values from the file index.
    constraints : dict[str, str]
        Required partition values.

    Returns
    -------
    bool
        True if all constraints are satisfied.
    """
    for col, required_val in constraints.items():
        actual_val = partition_values.get(col)
        if actual_val is None:
            return False
        if str(actual_val) != str(required_val):
            return False
    return True


def _deregister_table(ctx: SessionContext, table_name: str) -> None:
    """Safely deregister a table from the context.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context.
    table_name : str
        Name of the table to deregister.
    """
    deregister = getattr(ctx, "deregister_table", None)
    if callable(deregister):
        with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
            deregister(table_name)


__all__ = [
    "FilePruningPolicy",
    "FilePruningResult",
    "evaluate_and_select_files",
    "evaluate_filters_against_index",
    "select_candidate_files",
]
