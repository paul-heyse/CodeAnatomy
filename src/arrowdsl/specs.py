"""Shared specification types for joins, aggregates, and dedupe policies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SortKey:
    """Sort key specification for deterministic ordering.

    Parameters
    ----------
    column:
        Column name to sort by.
    order:
        Sort order ("ascending" or "descending").
    """

    column: str
    order: Literal["ascending", "descending"] = "ascending"


type DedupeStrategy = Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]


@dataclass(frozen=True)
class DedupeSpec:
    """Dedupe semantics for a table.

    Parameters
    ----------
    keys:
        Key columns that define duplicates.
    tie_breakers:
        Additional sort keys used for deterministic winner selection.
    strategy:
        Dedupe strategy name.
    """

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"


@dataclass(frozen=True)
class JoinSpec:
    """Join specification for hash joins.

    Parameters
    ----------
    join_type:
        Join type string.
    left_keys:
        Left-side join keys.
    right_keys:
        Right-side join keys.
    left_output:
        Output columns from the left side.
    right_output:
        Output columns from the right side.
    output_suffix_for_left:
        Suffix for left output column name collisions.
    output_suffix_for_right:
        Suffix for right output column name collisions.
    """

    join_type: str
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""

    def __post_init__(self) -> None:
        """Validate that left and right key counts match.

        Raises
        ------
        ValueError
            Raised when key lengths differ.
        """
        if len(self.left_keys) != len(self.right_keys):
            msg = "left_keys and right_keys must have the same length."
            raise ValueError(msg)


@dataclass(frozen=True)
class AggregateSpec:
    """Aggregate specification for group-by operations.

    Parameters
    ----------
    keys:
        Columns to group by.
    aggs:
        Aggregates as (column, function) pairs.
    use_threads:
        Whether to use threaded aggregation.
    rename_aggregates:
        When ``True``, rename aggregated columns back to their source names.
    """

    keys: tuple[str, ...]
    aggs: tuple[tuple[str, str], ...]
    use_threads: bool = True
    rename_aggregates: bool = False
