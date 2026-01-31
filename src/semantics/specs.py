"""Declarative semantic table specs for primary spans and ID derivations.

This module provides declarative specifications for:
- Table normalization (SpanBinding, IdDerivation, SemanticTableSpec)
- Relationship building (RelationshipSpec)

Relationships are declared with intent hints (overlap, contains, ownership)
that flow into join inference, allowing new relationships to be added
by spec changes only without pipeline edits.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from semantics.joins.strategies import JoinStrategyType

SpanUnit = Literal["byte"]
JoinHint = Literal["overlap", "contains", "ownership"]


@dataclass(frozen=True)
class SpanBinding:
    """Bind a semantic span role to concrete source columns."""

    start_col: str
    end_col: str
    unit: SpanUnit = "byte"
    canonical_start: str = "bstart"
    canonical_end: str = "bend"
    canonical_span: str = "span"


@dataclass(frozen=True)
class IdDerivation:
    """Derive an ID from path + span with a stable namespace."""

    out_col: str
    namespace: str
    path_col: str = "path"
    start_col: str = "bstart"
    end_col: str = "bend"
    null_if_any_null: bool = True
    canonical_entity_id: str | None = "entity_id"


@dataclass(frozen=True)
class ForeignKeyDerivation:
    """Derive a foreign-key ID using a reference span."""

    out_col: str
    target_namespace: str
    path_col: str = "path"
    start_col: str = ""
    end_col: str = ""
    null_if_any_null: bool = True
    guard_null_if: tuple[str, ...] = ()


@dataclass(frozen=True)
class SemanticTableSpec:
    """Declarative spec for semantic normalization."""

    table: str
    path_col: str = "path"
    primary_span: SpanBinding = field(default_factory=lambda: SpanBinding("bstart", "bend"))
    entity_id: IdDerivation = field(
        default_factory=lambda: IdDerivation(out_col="entity_id", namespace="entity")
    )
    foreign_keys: tuple[ForeignKeyDerivation, ...] = ()
    text_cols: tuple[str, ...] = ()


@dataclass(frozen=True)
class RelationshipSpec:
    """Declarative specification for a semantic relationship.

    Relationships connect two normalized tables (left entity table, right symbol
    table) using span-based joins. The join_hint guides inference to determine
    the appropriate join strategy:

    - "overlap": Spans that overlap in the same file (most common)
    - "contains": One span fully contains another (e.g., definition contains symbol)
    - "ownership": Logical ownership relationship (treated as overlap for joins)

    Attributes
    ----------
    name
        Output name for the relationship view (e.g., "rel_name_symbol").
    left_table
        Left (entity) table name. Must have entity_id column after normalization.
    right_table
        Right (symbol/reference) table name. Must have symbol column.
    join_hint
        Hint for join inference. Determines join strategy selection.
    origin
        Origin label for relationship edges. Used to trace edge provenance.
    filter_sql
        Optional SQL filter expression applied after the join.
        Uses column names from both tables (e.g., "is_read = true").

    Examples
    --------
    >>> spec = RelationshipSpec(
    ...     name="rel_name_symbol",
    ...     left_table="cst_refs_norm",
    ...     right_table="scip_occurrences_norm",
    ...     join_hint="overlap",
    ...     origin="cst_ref",
    ...     filter_sql="is_read = true",
    ... )
    """

    name: str
    left_table: str
    right_table: str
    join_hint: JoinHint = "overlap"
    origin: str = ""
    filter_sql: str | None = None

    def join_type(self) -> Literal["overlap", "contains"]:
        """Derive the join type from the hint.

        Returns
        -------
        Literal["overlap", "contains"]
            The join type to use for span-based joins.
        """
        if self.join_hint == "contains":
            return "contains"
        # Both "overlap" and "ownership" use overlap joins
        return "overlap"

    def to_strategy_type(self) -> JoinStrategyType:
        """Convert hint to a JoinStrategyType for inference.

        Returns
        -------
        JoinStrategyType
            The strategy type hint for join inference.
        """
        from semantics.joins.strategies import JoinStrategyType

        if self.join_hint == "contains":
            return JoinStrategyType.SPAN_CONTAINS
        return JoinStrategyType.SPAN_OVERLAP


__all__ = [
    "ForeignKeyDerivation",
    "IdDerivation",
    "JoinHint",
    "RelationshipSpec",
    "SemanticTableSpec",
    "SpanBinding",
    "SpanUnit",
]
