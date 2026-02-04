"""Join helper functions for semantic operations.

These are just functions, not a framework. They encode the three join patterns:
1. Path join - equijoin on path columns
2. Span overlap - path join + span_overlaps filter
3. Span contains - path join + span_contains filter
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datafusion import DataFrame

    from semantics.schema import SemanticSchema

JoinHow = Literal["inner", "left", "right", "full", "semi", "anti"]


def join_by_path(
    left: DataFrame,
    right: DataFrame,
    left_sem: SemanticSchema,
    right_sem: SemanticSchema,
    *,
    how: JoinHow = "inner",
) -> DataFrame:
    """Join two DataFrames on their path columns.

    Parameters
    ----------
    left
        Left DataFrame.
    right
        Right DataFrame.
    left_sem
        Semantic schema for left DataFrame.
    right_sem
        Semantic schema for right DataFrame.
    how
        Join type: "inner", "left", "right".

    Returns
    -------
    DataFrame
        Joined DataFrame with columns from both.
    """
    return left.join(
        right,
        left_on=[left_sem.path_name()],
        right_on=[right_sem.path_name()],
        how=how,
    )


def join_by_span_overlap(
    left: DataFrame,
    right: DataFrame,
    left_sem: SemanticSchema,
    right_sem: SemanticSchema,
) -> DataFrame:
    """Join on path, then filter by span overlap.

    Rule 5: IF table_A is EVIDENCE ∧ table_B is EVIDENCE ∧ can_path_join
    THEN join where spans overlap.

    Parameters
    ----------
    left
        Left DataFrame (must have path + span).
    right
        Right DataFrame (must have path + span).
    left_sem
        Semantic schema for left DataFrame.
    right_sem
        Semantic schema for right DataFrame.

    Returns
    -------
    DataFrame
        Joined DataFrame where spans overlap.
    """
    from datafusion_engine.udf.expr import udf_expr

    joined = join_by_path(left, right, left_sem, right_sem)
    overlap_filter = udf_expr("span_overlaps", left_sem.span_expr(), right_sem.span_expr())
    return joined.filter(overlap_filter)


def join_by_span_contains(
    left: DataFrame,
    right: DataFrame,
    left_sem: SemanticSchema,
    right_sem: SemanticSchema,
    *,
    left_contains_right: bool = True,
) -> DataFrame:
    """Join on path, then filter by span containment.

    Rule 6: IF table_A is EVIDENCE ∧ table_B is EVIDENCE ∧ can_path_join
    THEN join where one span contains the other.

    Parameters
    ----------
    left
        Left DataFrame (must have path + span).
    right
        Right DataFrame (must have path + span).
    left_sem
        Semantic schema for left DataFrame.
    right_sem
        Semantic schema for right DataFrame.
    left_contains_right
        If True, left span contains right span.
        If False, right span contains left span.

    Returns
    -------
    DataFrame
        Joined DataFrame where containment holds.
    """
    from datafusion_engine.udf.expr import udf_expr

    joined = join_by_path(left, right, left_sem, right_sem)
    if left_contains_right:
        contains_filter = udf_expr("span_contains", left_sem.span_expr(), right_sem.span_expr())
    else:
        contains_filter = udf_expr("span_contains", right_sem.span_expr(), left_sem.span_expr())
    return joined.filter(contains_filter)


__all__ = [
    "join_by_path",
    "join_by_span_contains",
    "join_by_span_overlap",
]
