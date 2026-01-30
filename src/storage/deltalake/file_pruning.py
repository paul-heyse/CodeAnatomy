"""File pruning policies and evaluation for Delta Lake tables."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as f

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.session.helpers import temp_table
from datafusion_engine.udf.shims import list_extract, map_extract

if TYPE_CHECKING:
    from datafusion.expr import Expr


@dataclass(frozen=True)
class PartitionFilter:
    """Partition-value filter for file pruning."""

    column: str
    op: Literal["=", "!=", "in", "not in"]
    value: str | Sequence[str]


@dataclass(frozen=True)
class StatsFilter:
    """Statistics range filter for file pruning."""

    column: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    value: Any
    cast_type: str | None = None


@dataclass(frozen=True)
class FilePruningPolicy(FingerprintableConfig):
    """File pruning policy with partition and statistics filters.

    Attributes
    ----------
    partition_filters : list[PartitionFilter]
        Partition-value filters applied to partition values.
    stats_filters : list[StatsFilter]
        Range filters applied to stats_min/stats_max metadata.
    """

    partition_filters: list[PartitionFilter]
    stats_filters: list[StatsFilter]

    def __post_init__(self) -> None:
        """Validate policy values.

        Raises
        ------
        TypeError
            Raised when partition or stats filters are not lists.
        """
        if not isinstance(self.partition_filters, list):
            msg = "partition_filters must be a list of PartitionFilter entries"
            raise TypeError(msg)
        if not isinstance(self.stats_filters, list):
            msg = "stats_filters must be a list of StatsFilter entries"
            raise TypeError(msg)

    def has_filters(self) -> bool:
        """Check if any filters are defined.

        Returns
        -------
        bool
            True if partition or stats filters are present.
        """
        return bool(self.partition_filters) or bool(self.stats_filters)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the file pruning policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing file pruning policy settings.
        """
        return {
            "partition_filters": [
                {
                    "column": filter_spec.column,
                    "op": filter_spec.op,
                    "value": (
                        tuple(filter_spec.value)
                        if isinstance(filter_spec.value, Sequence)
                        and not isinstance(filter_spec.value, str)
                        else filter_spec.value
                    ),
                }
                for filter_spec in self.partition_filters
            ],
            "stats_filters": [
                {
                    "column": filter_spec.column,
                    "op": filter_spec.op,
                    "value": filter_spec.value,
                    "cast_type": filter_spec.cast_type,
                }
                for filter_spec in self.stats_filters
            ],
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the file pruning policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

    def to_predicate(self) -> Expr | None:
        """Convert policy filters to a DataFusion predicate expression.

        Returns
        -------
        Expr | None
            Combined predicate expression, or None if no filters are defined.
        """
        all_filters: list[Expr] = []
        all_filters.extend(
            _partition_filter_expr(filter_spec) for filter_spec in self.partition_filters
        )
        all_filters.extend(_stats_filter_expr(filter_spec) for filter_spec in self.stats_filters)
        if not all_filters:
            return None
        combined = all_filters[0]
        for expr in all_filters[1:]:
            combined &= expr
        return combined


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
    """Evaluate pruning filters against the file index using DataFusion.

    This function registers the file index as a temporary table in DataFusion,
    applies DataFusion predicate expressions to filter files, and returns the
    filtered index.

    Parameters
    ----------
    index : pa.Table
        File index table from build_delta_file_index.
    policy : FilePruningPolicy
        Pruning policy with partition and statistics filters.
    ctx : SessionContext
        DataFusion session context for predicate evaluation.

    Returns
    -------
    pa.Table
        Filtered file index table with only candidate files.
    """
    if not policy.has_filters():
        return index

    predicate = policy.to_predicate()
    if predicate is None:
        return index

    with temp_table(ctx, index, prefix="__file_index_") as temp_table_name:
        df = ctx.table(temp_table_name).filter(predicate)
        return df.to_arrow_table()


def select_candidate_files(
    index: pa.Table,
    policy: FilePruningPolicy,
) -> list[str]:
    """Select candidate file paths from the index using Python filtering.

    This is a lightweight alternative to evaluate_filters_against_index that
    evaluates filters against partition values and statistics maps.

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

    # Filter files based on partition values
    candidate_paths: list[str] = []
    path_col = index.column("path")
    partition_values_col = index.column("partition_values")
    stats_min_col = index.column("stats_min")
    stats_max_col = index.column("stats_max")

    for i in range(index.num_rows):
        path = str(path_col[i].as_py())
        partition_values: dict[str, Any] | None = partition_values_col[i].as_py()

        if partition_values is None:
            partition_values = {}

        stats_min: dict[str, Any] | None = stats_min_col[i].as_py()
        stats_max: dict[str, Any] | None = stats_max_col[i].as_py()
        if stats_min is None:
            stats_min = {}
        if stats_max is None:
            stats_max = {}

        if not _matches_partition_filters(partition_values, policy.partition_filters):
            continue
        if not _matches_stats_filters(stats_min, stats_max, policy.stats_filters):
            continue

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
        Optional DataFusion session context for predicate-based filtering.
        If None, uses Python filtering.

    Returns
    -------
    FilePruningResult
        Pruning result with candidate files and statistics.
    """
    total_files = index.num_rows

    if ctx is not None and policy.has_filters():
        filtered_index = evaluate_filters_against_index(index, policy, ctx)
        candidate_paths = [str(p) for p in filtered_index.column("path").to_pylist()]
    else:
        candidate_paths = select_candidate_files(index, policy)

    candidate_count = len(candidate_paths)
    pruned_count = total_files - candidate_count

    return FilePruningResult(
        candidate_count=candidate_count,
        total_files=total_files,
        pruned_count=pruned_count,
        candidate_paths=candidate_paths,
    )


def _map_value_expr(map_col: str, key: str, *, cast_type: str | None = None) -> Expr:
    value_expr = list_extract(map_extract(col(map_col), key), 1)
    if cast_type is None:
        return value_expr
    return f.arrow_cast(value_expr, lit(cast_type))


def _partition_filter_expr(filter_spec: PartitionFilter) -> Expr:
    value_expr = _map_value_expr("partition_values", filter_spec.column, cast_type="Utf8")
    if filter_spec.op in {"in", "not in"}:
        if isinstance(filter_spec.value, str):
            values = [filter_spec.value]
        else:
            values = list(filter_spec.value)
        value_exprs = [lit(str(value)) for value in values]
        return f.in_list(value_expr, value_exprs, negated=filter_spec.op == "not in")
    expected = str(filter_spec.value)
    if filter_spec.op == "=":
        return value_expr == lit(expected)
    if filter_spec.op == "!=":
        return value_expr != lit(expected)
    msg = f"Unsupported partition filter op: {filter_spec.op}"
    raise ValueError(msg)


def _stats_filter_expr(filter_spec: StatsFilter) -> Expr:
    min_expr = _map_value_expr("stats_min", filter_spec.column, cast_type=filter_spec.cast_type)
    max_expr = _map_value_expr("stats_max", filter_spec.column, cast_type=filter_spec.cast_type)
    value_expr = (
        f.arrow_cast(lit(filter_spec.value), lit(filter_spec.cast_type))
        if filter_spec.cast_type is not None
        else lit(filter_spec.value)
    )
    if filter_spec.op == ">":
        return max_expr > value_expr
    if filter_spec.op == ">=":
        return max_expr >= value_expr
    if filter_spec.op == "<":
        return min_expr < value_expr
    if filter_spec.op == "<=":
        return min_expr <= value_expr
    if filter_spec.op == "=":
        return (min_expr <= value_expr) & (max_expr >= value_expr)
    if filter_spec.op == "!=":
        return ~((min_expr == value_expr) & (max_expr == value_expr))
    msg = f"Unsupported stats filter op: {filter_spec.op}"
    raise ValueError(msg)


def _resolve_filter_value(value: Any, cast_type: str | None) -> Any:
    if value is None or cast_type is None:
        return value
    if cast_type in {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}:
        with contextlib.suppress(TypeError, ValueError):
            return int(value)
    if cast_type in {"Float32", "Float64"}:
        with contextlib.suppress(TypeError, ValueError):
            return float(value)
    if cast_type == "Boolean":
        if isinstance(value, str):
            return value.lower() == "true"
        return bool(value)
    return str(value)


def _matches_partition_filters(
    partition_values: dict[str, Any],
    filters: Sequence[PartitionFilter],
) -> bool:
    for filter_spec in filters:
        actual = partition_values.get(filter_spec.column)
        if actual is None:
            return False
        actual_text = str(actual)
        if filter_spec.op == "=" and actual_text != str(filter_spec.value):
            return False
        if filter_spec.op == "!=" and actual_text == str(filter_spec.value):
            return False
        if filter_spec.op in {"in", "not in"}:
            values = (
                [filter_spec.value]
                if isinstance(filter_spec.value, str)
                else list(filter_spec.value)
            )
            value_set = {str(value) for value in values}
            matches = actual_text in value_set
            if filter_spec.op == "in" and not matches:
                return False
            if filter_spec.op == "not in" and matches:
                return False
    return True


def _matches_stats_filters(
    stats_min: dict[str, Any],
    stats_max: dict[str, Any],
    filters: Sequence[StatsFilter],
) -> bool:
    matched = True
    for filter_spec in filters:
        min_value = _resolve_filter_value(stats_min.get(filter_spec.column), filter_spec.cast_type)
        max_value = _resolve_filter_value(stats_max.get(filter_spec.column), filter_spec.cast_type)
        target = _resolve_filter_value(filter_spec.value, filter_spec.cast_type)
        if min_value is None or max_value is None or target is None:
            continue
        if filter_spec.op == ">" and not (max_value > target):
            matched = False
            continue
        if filter_spec.op == ">=" and not (max_value >= target):
            matched = False
            continue
        if filter_spec.op == "<" and not (min_value < target):
            matched = False
            continue
        if filter_spec.op == "<=" and not (min_value <= target):
            matched = False
            continue
        if filter_spec.op == "=" and not (min_value <= target <= max_value):
            matched = False
            continue
        if filter_spec.op == "!=" and min_value == target and max_value == target:
            matched = False
            continue
    return matched


__all__ = [
    "FilePruningPolicy",
    "FilePruningResult",
    "PartitionFilter",
    "StatsFilter",
    "evaluate_and_select_files",
    "evaluate_filters_against_index",
    "select_candidate_files",
]
