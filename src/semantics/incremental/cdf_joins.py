"""Change-data-feed join framework for incremental semantic updates.

This module provides a CDF-based join framework that enables incremental
relationship recomputation in the semantic pipeline. It integrates with
the semantic CDF filter types.

The framework supports multiple merge strategies for combining CDF changes
with existing data, and can automatically detect whether a DataFrame has
CDF information available.

Example usage::

    from semantics.incremental import CDFJoinSpec, CDFMergeStrategy, build_incremental_join

    spec = CDFJoinSpec(
        left_table="cst_refs_norm_v1",
        right_table="scip_occurrences_norm_v1",
        output_name="rel_name_symbol_incremental",
        key_columns=("file_id", "bstart", "bend"),
        merge_strategy=CDFMergeStrategy.UPSERT,
    )

    result = build_incremental_join(
        ctx,
        spec,
        left_df=refs_df,
        right_df=scip_df,
        join_builder=compiler.relate,
    )
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import TYPE_CHECKING

from semantics.incremental.cdf_types import CdfFilterPolicy

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


# -----------------------------------------------------------------------------
# CDF Merge Strategies
# -----------------------------------------------------------------------------


class CDFMergeStrategy(StrEnum):
    """Strategy for merging CDF changes with existing data.

    These strategies determine how incremental results are combined with
    existing materialized data when performing incremental updates.

    Attributes:
    ----------
    APPEND
        Add new rows without removing anything. Suitable for insert-only
        workloads where rows are never updated or deleted.
    UPSERT
        Update existing rows and insert new ones based on key columns.
        This is the most common strategy for relationship tables.
    REPLACE
        Replace all data matching key columns with new data. Useful when
        a full refresh of a subset of data is needed.
    DELETE_INSERT
        Delete matching rows first, then insert new rows. Provides
        explicit control over the deletion and insertion phases.
    """

    APPEND = auto()
    UPSERT = auto()
    REPLACE = auto()
    DELETE_INSERT = auto()


# -----------------------------------------------------------------------------
# CDF Join Specification
# -----------------------------------------------------------------------------

# Default CDF column name used by Delta Lake
DEFAULT_CDF_COLUMN = "_change_type"


@dataclass(frozen=True)
class CDFJoinSpec:
    """Specification for an incremental CDF-based join.

    This specification defines how two tables with potential CDF changes
    should be joined for incremental relationship recomputation.

    Attributes:
    ----------
    left_table
        Base table name for the join (may include CDF changes).
    right_table
        Join target table name (may include CDF changes).
    output_name
        Name for the incremental output view or table.
    key_columns
        Columns that identify unique rows for merge operations.
        Used to determine row identity during upsert/replace operations.
    merge_strategy
        How to merge incremental results with existing data.
        Defaults to UPSERT for typical relationship updates.
    cdf_column
        Column name identifying CDF operation type (insert/update/delete).
        Defaults to ``_change_type`` as used by Delta Lake CDF.
    filter_policy
        Optional policy for filtering CDF changes. If not provided,
        defaults to including inserts and updates but excluding deletes.
    """

    left_table: str
    right_table: str
    output_name: str
    key_columns: tuple[str, ...]
    merge_strategy: CDFMergeStrategy = CDFMergeStrategy.UPSERT
    cdf_column: str = DEFAULT_CDF_COLUMN
    filter_policy: CdfFilterPolicy | None = None

    def effective_filter_policy(self) -> CdfFilterPolicy:
        """Return the effective CDF filter policy.

        If no filter policy is explicitly provided, returns a default policy
        that includes inserts and updates but excludes deletes.

        Returns:
        -------
        CdfFilterPolicy
            The filter policy to use for CDF filtering.
        """
        if self.filter_policy is not None:
            return self.filter_policy
        # Default: include inserts and updates, exclude deletes
        return CdfFilterPolicy.inserts_and_updates_only()


# -----------------------------------------------------------------------------
# CDF Detection
# -----------------------------------------------------------------------------


def is_cdf_enabled(df: DataFrame, cdf_column: str = DEFAULT_CDF_COLUMN) -> bool:
    """Check if a DataFrame has CDF information.

    Parameters
    ----------
    df
        DataFrame to check.
    cdf_column
        Expected CDF column name. Defaults to ``_change_type``.

    Returns:
    -------
    bool
        True if the CDF column exists in the DataFrame schema.

    Examples:
    --------
    >>> if is_cdf_enabled(refs_df):
    ...     # Use incremental join path
    ...     result = build_incremental_join(ctx, spec, ...)
    ... else:
    ...     # Use full refresh path
    ...     result = compiler.relate(...)
    """
    schema = df.schema()
    return any(field.name == cdf_column for field in schema)


def _has_cdf_column(schema_fields: list[object], cdf_column: str) -> bool:
    """Check if schema fields include the CDF column.

    Parameters
    ----------
    schema_fields
        List of schema fields (with name attribute).
    cdf_column
        Expected CDF column name.

    Returns:
    -------
    bool
        True if the CDF column exists.
    """
    return any(getattr(field, "name", None) == cdf_column for field in schema_fields)


# -----------------------------------------------------------------------------
# CDF Filtering
# -----------------------------------------------------------------------------


def _apply_cdf_filter(
    df: DataFrame,
    *,
    filter_policy: CdfFilterPolicy,
    cdf_column: str = DEFAULT_CDF_COLUMN,
) -> DataFrame:
    """Filter DataFrame according to CDF filter policy.

    Applies the filter policy predicate to exclude unwanted CDF change types.
    If the DataFrame does not have a CDF column, returns it unchanged
    (full refresh mode).

    Parameters
    ----------
    df
        DataFrame with potential CDF column.
    filter_policy
        Policy determining which change types to include.
    cdf_column
        Name of the CDF operation type column.

    Returns:
    -------
    DataFrame
        Filtered DataFrame with only the desired CDF change types.

    Notes:
    -----
    This function checks for CDF column existence before filtering.
    DataFrames without CDF columns pass through unchanged, enabling
    transparent handling of both incremental and full-refresh data.
    """
    if not is_cdf_enabled(df, cdf_column):
        # No CDF column - return as-is (full refresh mode)
        return df

    predicate = filter_policy.to_datafusion_predicate()
    if predicate is None:
        # All change types included - no filtering needed
        return df

    return df.filter(predicate)


# -----------------------------------------------------------------------------
# Incremental Join Builder
# -----------------------------------------------------------------------------


def build_incremental_join(
    ctx: SessionContext,
    spec: CDFJoinSpec,
    *,
    left_df: DataFrame,
    right_df: DataFrame,
    join_builder: Callable[[DataFrame, DataFrame], DataFrame],
) -> DataFrame:
    """Build an incremental join using CDF changes.

    Filters CDF rows according to the spec's filter policy and performs
    the join using the provided join builder function.

    Parameters
    ----------
    ctx
        DataFusion session context.
    spec
        CDF join specification defining tables, keys, and merge strategy.
    left_df
        Left DataFrame (potentially with CDF changes).
    right_df
        Right DataFrame (potentially with CDF changes).
    join_builder
        Callable that performs the actual join operation.
        Typically ``SemanticCompiler.relate`` or similar.

    Returns:
    -------
    DataFrame
        Joined result with CDF rows filtered appropriately.

    Notes:
    -----
    This is a simplified implementation for the semantic pipeline.
    Full incremental processing would require:

    1. Tracking which rows changed via CDF
    2. Only recomputing affected joins
    3. Merging incremental results with existing data

    For now, we filter out deletes and perform a full join on the
    filtered data. This provides correctness while future optimizations
    can add true incremental computation.

    The ``ctx`` parameter is included for future extensions that may
    need to register intermediate views or access session state.

    Examples:
    --------
    >>> spec = CDFJoinSpec(
    ...     left_table="refs",
    ...     right_table="symbols",
    ...     output_name="rel_incremental",
    ...     key_columns=("file_id", "bstart"),
    ... )
    >>> result = build_incremental_join(
    ...     ctx,
    ...     spec,
    ...     left_df=refs_df,
    ...     right_df=symbols_df,
    ...     join_builder=lambda l, r: compiler.relate(l, r, join_type="overlap"),
    ... )
    """
    # Suppress unused variable warning - ctx reserved for future extensions
    _ = ctx

    filter_policy = spec.effective_filter_policy()
    cdf_column = spec.cdf_column

    # Filter out unwanted CDF change types from both sides
    filtered_left = _apply_cdf_filter(left_df, filter_policy=filter_policy, cdf_column=cdf_column)
    filtered_right = _apply_cdf_filter(right_df, filter_policy=filter_policy, cdf_column=cdf_column)

    # Perform the join using the provided builder
    return join_builder(filtered_left, filtered_right)


# -----------------------------------------------------------------------------
# CDF Merge Strategies
# -----------------------------------------------------------------------------


def apply_cdf_merge(
    existing: DataFrame,
    new_data: DataFrame,
    *,
    key_columns: tuple[str, ...],
    strategy: CDFMergeStrategy,
    partition_column: str | None = None,
) -> DataFrame:
    """Apply a CDF merge strategy to combine existing and new data.

    Args:
        existing: Description.
            new_data: Description.
            key_columns: Description.
            strategy: Description.
            partition_column: Description.

    Returns:
        DataFrame: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    from datafusion import col

    if strategy == CDFMergeStrategy.APPEND:
        # Simple union - no deduplication
        return existing.union(new_data)

    if strategy == CDFMergeStrategy.UPSERT:
        # Anti-join to remove matching rows, then union
        pruned = existing.join(new_data, key_columns, how="anti")
        return pruned.union(new_data)

    if strategy == CDFMergeStrategy.REPLACE:
        # Partition replacement - remove all rows in affected partitions
        if partition_column is None:
            msg = (
                "REPLACE strategy requires partition_column to be specified. "
                "The partition column determines which rows to replace."
            )
            raise ValueError(msg)

        # Extract distinct partition values from new data
        affected_partitions = new_data.select(col(partition_column)).distinct()

        # Anti-join by partition column to remove all rows in affected partitions
        pruned = existing.join(
            affected_partitions,
            (partition_column,),
            how="anti",
        )
        return pruned.union(new_data)

    if strategy == CDFMergeStrategy.DELETE_INSERT:
        # Same as UPSERT but with explicit delete-then-insert semantics
        pruned = existing.join(new_data, key_columns, how="anti")
        return pruned.union(new_data)

    msg = f"Unsupported merge strategy: {strategy}"
    raise ValueError(msg)


def merge_incremental_results(
    ctx: SessionContext,
    *,
    incremental_df: DataFrame,
    base_table: str,
    key_columns: tuple[str, ...],
    strategy: CDFMergeStrategy = CDFMergeStrategy.UPSERT,
) -> DataFrame:
    """Merge incremental results into an existing base table.

    Parameters
    ----------
    ctx
        DataFusion session context.
    incremental_df
        Incremental results to merge.
    base_table
        Base table name to merge into.
    key_columns
        Join keys used to identify rows to replace.
    strategy
        Merge strategy to apply. Defaults to UPSERT for backward compatibility.

    Returns:
    -------
    DataFrame
        Merged result with base rows replaced by incremental rows.

    Notes:
    -----
    This function delegates to :func:`apply_cdf_merge` when the base table exists.
    For tables not yet in the session, returns the incremental data directly.
    """
    from datafusion_engine.schema.introspection import table_names_snapshot

    if base_table not in table_names_snapshot(ctx):
        return incremental_df
    base_df = ctx.table(base_table)
    return apply_cdf_merge(
        base_df,
        incremental_df,
        key_columns=key_columns,
        strategy=strategy,
        partition_column=None,
    )


def incremental_join_enabled(
    left_df: DataFrame,
    right_df: DataFrame,
    cdf_column: str = DEFAULT_CDF_COLUMN,
) -> bool:
    """Check if incremental join processing is available.

    Returns True if at least one of the input DataFrames has CDF data,
    indicating that incremental processing may be beneficial.

    Parameters
    ----------
    left_df
        Left DataFrame for the join.
    right_df
        Right DataFrame for the join.
    cdf_column
        Expected CDF column name.

    Returns:
    -------
    bool
        True if either DataFrame has CDF information.
    """
    return is_cdf_enabled(left_df, cdf_column) or is_cdf_enabled(right_df, cdf_column)


__all__ = [
    "DEFAULT_CDF_COLUMN",
    "CDFJoinSpec",
    "CDFMergeStrategy",
    "apply_cdf_merge",
    "build_incremental_join",
    "incremental_join_enabled",
    "is_cdf_enabled",
    "merge_incremental_results",
]
