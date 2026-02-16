"""Join strategies for semantic relationships.

Defines the join strategy types and standard strategy instances used by
the join inference system. Each strategy encapsulates the join type,
key columns, and optional filter expressions.

Strategy Types
--------------
EQUI_JOIN
    Simple key equality join (e.g., file_id = file_id).
SPAN_OVERLAP
    Spans that overlap in the same file (requires file identity + spans).
SPAN_CONTAINS
    One span contains another (inner span fully within outer span).
FOREIGN_KEY
    FK relationship where one column references another's primary key.
SYMBOL_MATCH
    Symbol name matching for symbol-to-symbol joins.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto


class JoinStrategyType(StrEnum):
    """Types of join strategies.

    Each type represents a semantic join pattern that can be inferred
    from schema annotations.
    """

    EQUI_JOIN = auto()  # Simple key equality (file_id = file_id)
    SPAN_OVERLAP = auto()  # Spans overlap in same file
    SPAN_CONTAINS = auto()  # One span contains another
    FOREIGN_KEY = auto()  # FK relationship (entity_id -> def_id)
    SYMBOL_MATCH = auto()  # Symbol name matching


@dataclass(frozen=True)
class JoinStrategy:
    """Specification for how to join two tables.

    Attributes:
    ----------
    strategy_type
        The type of join strategy.
    left_keys
        Column names from left table used in the join.
    right_keys
        Column names from right table used in the join.
    filter_expr
        Optional SQL filter expression applied after join.
    confidence
        Inference confidence score in [0.0, 1.0].  Higher values
        indicate stronger evidence backing the strategy selection.
    """

    strategy_type: JoinStrategyType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    filter_expr: str | None = None
    confidence: float = 1.0

    def __post_init__(self) -> None:
        """Validate join key presence and confidence bounds.

        Raises:
            ValueError: If keys are empty or confidence is outside ``[0.0, 1.0]``.
        """
        if not self.left_keys:
            msg = "JoinStrategy requires non-empty left_keys."
            raise ValueError(msg)
        if not self.right_keys:
            msg = "JoinStrategy requires non-empty right_keys."
            raise ValueError(msg)
        if not 0.0 <= self.confidence <= 1.0:
            msg = f"JoinStrategy confidence must be in [0.0, 1.0], got {self.confidence}."
            raise ValueError(msg)

    def describe(self) -> str:
        """Generate human-readable description of the strategy.

        Returns:
        -------
        str
            Description of join strategy including keys and conditions.
        """
        if self.strategy_type == JoinStrategyType.EQUI_JOIN:
            pairs = zip(self.left_keys, self.right_keys, strict=True)
            return f"EquiJoin on {', '.join(f'{left}={right}' for left, right in pairs)}"
        if self.strategy_type == JoinStrategyType.SPAN_OVERLAP:
            return "SpanOverlap (left.bstart < right.bend AND right.bstart < left.bend)"
        if self.strategy_type == JoinStrategyType.SPAN_CONTAINS:
            return "SpanContains (inner.bstart >= outer.bstart AND inner.bend <= outer.bend)"
        if self.strategy_type == JoinStrategyType.FOREIGN_KEY:
            return f"ForeignKey {self.left_keys} -> {self.right_keys}"
        if self.strategy_type == JoinStrategyType.SYMBOL_MATCH:
            pairs = zip(self.left_keys, self.right_keys, strict=True)
            return f"SymbolMatch on {', '.join(f'{left}={right}' for left, right in pairs)}"
        return f"Unknown({self.strategy_type})"

    def with_filter(self, filter_expr: str) -> JoinStrategy:
        """Create new strategy with additional filter expression.

        Parameters
        ----------
        filter_expr
            SQL filter expression to apply after join.

        Returns:
        -------
        JoinStrategy
            New strategy with filter expression.
        """
        combined = f"({self.filter_expr}) AND ({filter_expr})" if self.filter_expr else filter_expr
        return JoinStrategy(
            strategy_type=self.strategy_type,
            left_keys=self.left_keys,
            right_keys=self.right_keys,
            filter_expr=combined,
            confidence=self.confidence,
        )


# -----------------------------------------------------------------------------
# Standard strategies
# -----------------------------------------------------------------------------

FILE_EQUI_JOIN = JoinStrategy(
    strategy_type=JoinStrategyType.EQUI_JOIN,
    left_keys=("file_id",),
    right_keys=("file_id",),
)
"""Standard file-based equi-join on file_id columns."""

SPAN_OVERLAP_STRATEGY = JoinStrategy(
    strategy_type=JoinStrategyType.SPAN_OVERLAP,
    left_keys=("file_id", "bstart", "bend"),
    right_keys=("file_id", "bstart", "bend"),
    filter_expr="left.bstart < right.bend AND right.bstart < left.bend",
)
"""Standard span overlap strategy for same-file span intersection."""

SPAN_CONTAINS_STRATEGY = JoinStrategy(
    strategy_type=JoinStrategyType.SPAN_CONTAINS,
    left_keys=("file_id", "bstart", "bend"),
    right_keys=("file_id", "bstart", "bend"),
    filter_expr="inner.bstart >= outer.bstart AND inner.bend <= outer.bend",
)
"""Standard span containment strategy (inner span within outer span)."""


def make_fk_strategy(
    left_fk_col: str,
    right_pk_col: str,
) -> JoinStrategy:
    """Create a foreign key join strategy.

    Parameters
    ----------
    left_fk_col
        Foreign key column in left table.
    right_pk_col
        Primary key column in right table.

    Returns:
    -------
    JoinStrategy
        Foreign key join strategy.
    """
    return JoinStrategy(
        strategy_type=JoinStrategyType.FOREIGN_KEY,
        left_keys=(left_fk_col,),
        right_keys=(right_pk_col,),
    )


def make_symbol_match_strategy(
    left_col: str = "symbol",
    right_col: str = "symbol",
) -> JoinStrategy:
    """Create a symbol matching join strategy.

    Parameters
    ----------
    left_col
        Symbol column in left table.
    right_col
        Symbol column in right table.

    Returns:
    -------
    JoinStrategy
        Symbol match join strategy.
    """
    return JoinStrategy(
        strategy_type=JoinStrategyType.SYMBOL_MATCH,
        left_keys=(left_col,),
        right_keys=(right_col,),
    )


__all__ = [
    "FILE_EQUI_JOIN",
    "SPAN_CONTAINS_STRATEGY",
    "SPAN_OVERLAP_STRATEGY",
    "JoinStrategy",
    "JoinStrategyType",
    "make_fk_strategy",
    "make_symbol_match_strategy",
]
