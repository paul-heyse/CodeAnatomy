"""Plan specs and shared operation metadata for ArrowDSL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.context import ExecutionContext, OrderingEffect


def scan_ordering_effect(ctx: ExecutionContext) -> OrderingEffect:
    """Return the default scan ordering effect for a context.

    Returns
    -------
    OrderingEffect
        Ordering effect implied by the execution context.
    """
    if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output:
        return OrderingEffect.IMPLICIT
    return OrderingEffect.UNORDERED


@dataclass(frozen=True)
class SortKey:
    """Sort key specification for deterministic ordering."""

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
    """Dedupe semantics for a table."""

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"


@dataclass(frozen=True)
class IntervalAlignOptions:
    """Interval alignment configuration."""

    mode: Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"] = "CONTAINED_BEST"
    how: Literal["inner", "left"] = "inner"

    left_path_col: str = "path"
    left_start_col: str = "bstart"
    left_end_col: str = "bend"

    right_path_col: str = "path"
    right_start_col: str = "bstart"
    right_end_col: str = "bend"

    select_left: tuple[str, ...] = ()
    select_right: tuple[str, ...] = ()

    tie_breakers: tuple[SortKey, ...] = ()

    emit_match_meta: bool = True
    match_kind_col: str = "match_kind"
    match_score_col: str = "match_score"
    right_suffix: str = "__r"


@dataclass(frozen=True)
class AsofJoinSpec:
    """As-of join specification for nearest-match joins."""

    on: str
    by: tuple[str, ...] = ()
    tolerance: object | None = None
    right_on: str | None = None
    right_by: tuple[str, ...] = ()


type JoinType = Literal[
    "inner",
    "left outer",
    "right outer",
    "full outer",
    "left semi",
    "right semi",
    "left anti",
    "right anti",
]


@dataclass(frozen=True)
class JoinSpec:
    """Join specification for hash joins."""

    join_type: JoinType
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
            Raised when the key counts do not match.
        """
        if len(self.left_keys) != len(self.right_keys):
            msg = "left_keys and right_keys must have the same length."
            raise ValueError(msg)


__all__ = [
    "AsofJoinSpec",
    "DedupeSpec",
    "DedupeStrategy",
    "IntervalAlignOptions",
    "JoinSpec",
    "JoinType",
    "SortKey",
    "scan_ordering_effect",
]
