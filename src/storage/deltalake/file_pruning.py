"""File pruning policies and evaluation for Delta Lake tables."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.introspection import invalidate_introspection_cache
from sqlglot_tools.compat import Expression, exp


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

    Raises
    ------
    ValueError
        Raised when SQL execution does not return a DataFusion DataFrame.
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
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
        adapter.register_record_batches(temp_table_name, [list(index.to_batches())])

        # Construct and execute filter expression
        expr = _build_filter_expr(temp_table_name, policy)
        from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
        from datafusion_engine.execution_facade import DataFusionExecutionFacade
        from datafusion_engine.sql_options import sql_options_for_profile

        facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
        plan = facade.compile(
            expr,
            options=DataFusionCompileOptions(
                sql_options=sql_options_for_profile(None),
                sql_policy=DataFusionSqlPolicy(),
            ),
        )
        result = facade.execute(plan)
        if result.dataframe is None:
            msg = "File pruning SQL did not return a DataFusion DataFrame."
            raise ValueError(msg)
        return result.dataframe.to_arrow_table()
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


def _build_filter_expr(table_name: str, policy: FilePruningPolicy) -> Expression:
    """Build SQLGlot filter expression for file index.

    Parameters
    ----------
    table_name : str
        Name of the registered file index table.
    policy : FilePruningPolicy
        Pruning policy with filters.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot select expression.
    """
    predicate = _combine_predicates((*policy.partition_filters, *policy.stats_filters))
    select_expr = exp.select(exp.Star()).from_(table_name)
    if predicate is None:
        return select_expr
    return select_expr.where(predicate)


def _combine_predicates(filters: Sequence[str]) -> Expression | None:
    predicates: list[Expression] = []
    for raw in filters:
        text = str(raw).strip()
        if not text:
            continue
        predicates.append(_parse_predicate(text))
    if not predicates:
        return None
    if len(predicates) == 1:
        return predicates[0]
    return exp.and_(*predicates)


def _parse_predicate(predicate: str) -> Expression:
    from sqlglot.errors import ParseError

    from datafusion_engine.compile_options import DataFusionCompileOptions
    from sqlglot_tools.optimizer import (
        StrictParseOptions,
        parse_sql_strict,
        register_datafusion_dialect,
    )

    register_datafusion_dialect()
    try:
        expr = parse_sql_strict(
            f"SELECT * FROM __file_index WHERE {predicate}",
            dialect=DataFusionCompileOptions().dialect,
            options=StrictParseOptions(error_level=None),
        )
    except (ParseError, TypeError, ValueError) as exc:
        msg = f"Failed to parse pruning predicate: {predicate!r}."
        raise ValueError(msg) from exc
    where = expr.args.get("where")
    condition = getattr(where, "this", None)
    if not isinstance(condition, Expression):
        msg = f"Failed to resolve pruning predicate expression: {predicate!r}."
        raise TypeError(msg)
    return condition


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
            invalidate_introspection_cache(ctx)


__all__ = [
    "FilePruningPolicy",
    "FilePruningResult",
    "evaluate_and_select_files",
    "evaluate_filters_against_index",
    "select_candidate_files",
]
