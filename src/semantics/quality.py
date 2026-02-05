"""Core dataclasses for quality-aware relationship building.

This module defines the three-tier signal model for relationship quality:
1. Hard Keys - Required predicates for candidate filtering
2. Soft Evidence - Features that contribute to score computation
3. Quality Signals - File-level quality that adjusts confidence

All specifications are immutable (frozen dataclasses) and use ExprSpec
callables instead of SQL strings for type safety and composability.

Usage
-----
>>> from semantics.quality import QualityRelationshipSpec, SignalsSpec, Feature

>>> spec = QualityRelationshipSpec(
...     name="rel_docstring_owner_v1",
...     left_view="cst_docstrings_norm_v1",
...     right_view="cst_defs_norm_v1",
...     signals=SignalsSpec(
...         base_score=1000,
...         base_confidence=0.98,
...     ),
... )
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from semantics.exprs import ExprSpec


FeatureKind = Literal["evidence", "quality"]
"""Feature kinds: evidence (soft signals) or quality (file quality)."""

JoinHow = Literal["inner", "left", "right", "full"]
"""Join types for relationship building."""

RankKeep = Literal["best", "all"]
"""Ranking modes: keep only best match or all matches."""


@dataclass(frozen=True)
class SelectExpr:
    """Projected expression with explicit alias.

    Used to specify output columns from relationship compilation.

    Attributes
    ----------
    expr
        Expression spec to project.
    alias
        Output column name.

    Examples
    --------
    >>> from semantics.exprs import c, v
    >>> select = SelectExpr(c("ds_entity_id"), "src")
    """

    expr: ExprSpec
    alias: str


@dataclass(frozen=True)
class OrderSpec:
    """Order by expression with direction.

    Used to specify ranking order for ambiguity resolution.

    Attributes
    ----------
    expr
        Sort expression spec.
    direction
        Sort direction ("asc" or "desc").

    Examples
    --------
    >>> from semantics.exprs import c
    >>> order = OrderSpec(c("score"), direction="desc")
    """

    expr: ExprSpec
    direction: Literal["asc", "desc"] = "desc"


@dataclass(frozen=True)
class HardPredicate:
    """Boolean predicate for candidate filtering.

    Hard predicates are required conditions that must be satisfied
    for a candidate pair to be considered. They filter before scoring.

    Attributes
    ----------
    predicate
        Expression spec that returns a boolean.

    Examples
    --------
    >>> from semantics.exprs import is_not_null, eq
    >>> pred = HardPredicate(is_not_null("owner_def_id"))
    >>> pred2 = HardPredicate(eq("owner_def_id", "def_id"))
    """

    predicate: ExprSpec


@dataclass(frozen=True)
class Feature:
    """Numeric feature with weight and kind.

    Features contribute to the overall score computation. Evidence
    features represent soft matching signals, while quality features
    represent file-level quality adjustments.

    Attributes
    ----------
    name
        Feature name (used as column alias).
    expr
        Expression spec that returns a numeric value.
    weight
        Multiplier for this feature in score computation.
    kind
        Feature kind: "evidence" or "quality".

    Examples
    --------
    >>> from semantics.exprs import case_eq
    >>> feature = Feature("kind_match", case_eq("owner_kind", "def_kind"), weight=5.0)
    """

    name: str
    expr: ExprSpec
    weight: float = 1.0
    kind: FeatureKind = "evidence"


@dataclass(frozen=True)
class SignalsSpec:
    """Three-tier signal specification for quality-aware relationships.

    Combines hard predicates, soft features, and base scores into a
    complete signal specification for relationship quality computation.

    Attributes
    ----------
    base_score
        Base score before feature adjustments.
    base_confidence
        Base confidence before quality adjustments.
    hard
        Required predicates for candidate filtering.
    features
        Numeric features with weights for score computation.
    quality_score_column
        Column name for file quality score (if joining file_quality).
    quality_weight
        Weight for file quality contribution to confidence.

    Examples
    --------
    >>> from semantics.exprs import is_not_null, eq, case_eq
    >>> signals = SignalsSpec(
    ...     base_score=1000,
    ...     base_confidence=0.95,
    ...     hard=[
    ...         HardPredicate(is_not_null("owner_def_id")),
    ...         HardPredicate(eq("owner_def_id", "def_id")),
    ...     ],
    ...     features=[
    ...         Feature("kind_match", case_eq("owner_kind", "def_kind"), weight=5.0),
    ...     ],
    ... )
    """

    base_score: float = 1000.0
    base_confidence: float = 0.5
    hard: Sequence[HardPredicate] = ()
    features: Sequence[Feature] = ()
    quality_score_column: str = "file_quality_score"
    quality_weight: float = 0.0001


@dataclass(frozen=True)
class RankSpec:
    """Deterministic ranking and ambiguity grouping.

    Specifies how to rank candidates and select winners when multiple
    matches exist for the same source entity.

    Attributes
    ----------
    ambiguity_key_expr
        Expression spec for grouping ambiguous matches.
    ambiguity_group_id_expr
        Optional expression for the ambiguity group identifier. When provided,
        this expression is used to populate ``ambiguity_group_id`` and for
        ranking partitions.
    order_by
        Sort expressions for ranking within groups.
    keep
        Whether to keep only "best" or "all" matches.
    top_k
        Number of top matches to keep per group (when keep="best").

    Examples
    --------
    >>> from semantics.exprs import c
    >>> rank = RankSpec(
    ...     ambiguity_key_expr=c("ds_entity_id"),
    ...     ambiguity_group_id_expr=c("ds_entity_id"),
    ...     order_by=[
    ...         OrderSpec(c("score"), direction="desc"),
    ...         OrderSpec(c("def_bstart"), direction="asc"),
    ...     ],
    ...     keep="best",
    ...     top_k=1,
    ... )
    """

    ambiguity_key_expr: ExprSpec
    ambiguity_group_id_expr: ExprSpec | None = None
    order_by: Sequence[OrderSpec] = ()
    keep: RankKeep = "best"
    top_k: int = 1


@dataclass(frozen=True)
class QualityRelationshipSpec:
    """Complete relationship specification with quality signals.

    This is the main specification type for quality-aware relationship
    compilation. It defines the join, signals, ranking, and output
    projection for a relationship.

    Note: This is a NEW TYPE that coexists with the existing
    RelationshipSpec in specs.py. Existing specs continue to work,
    and quality specs can be migrated incrementally.

    Attributes
    ----------
    name
        Output name for the relationship view.
    left_view
        Left (entity) view name.
    right_view
        Right (symbol/reference) view name.
    left_on
        Columns for equi-join from left view.
    right_on
        Columns for equi-join from right view.
    how
        Join type: "inner", "left", "right", or "full".
    signals
        Three-tier signal specification.
    origin
        Origin label for relationship edges.
    provider
        Provider name for the relationship source.
    rule_name
        Optional rule name for debugging.
    rank
        Optional ranking specification for ambiguity resolution.
    select_exprs
        Output column expressions with aliases.
    join_file_quality
        Whether to join file_quality for quality signals.
    file_quality_view
        Name of file quality view to join.

    Examples
    --------
    >>> from semantics.exprs import c, v, eq, is_not_null, case_eq
    >>> spec = QualityRelationshipSpec(
    ...     name="rel_cst_docstring_owner_by_id",
    ...     left_view="cst_docstrings_norm",
    ...     right_view="cst_defs_norm",
    ...     left_on=["file_id"],
    ...     right_on=["file_id"],
    ...     how="inner",
    ...     provider="libcst",
    ...     signals=SignalsSpec(
    ...         base_score=1000,
    ...         base_confidence=0.98,
    ...         hard=[
    ...             HardPredicate(is_not_null("owner_def_id")),
    ...             HardPredicate(eq("owner_def_id", "def_id")),
    ...         ],
    ...         features=[
    ...             Feature("kind_match", case_eq("owner_kind", "def_kind"), weight=5.0),
    ...         ],
    ...     ),
    ...     rank=RankSpec(
    ...         ambiguity_key_expr=c("ds_entity_id"),
    ...         order_by=[OrderSpec(c("score"), direction="desc")],
    ...         keep="best",
    ...         top_k=1,
    ...     ),
    ...     select_exprs=[
    ...         SelectExpr(c("ds_entity_id"), "src"),
    ...         SelectExpr(c("def_entity_id"), "dst"),
    ...         SelectExpr(v("has_docstring"), "kind"),
    ...     ],
    ... )
    """

    name: str
    left_view: str
    right_view: str

    # Equi-join keys
    left_on: Sequence[str] = ()
    right_on: Sequence[str] = ()
    how: JoinHow = "inner"

    # Three-tier signals
    signals: SignalsSpec = field(default_factory=SignalsSpec)

    # Contract/provenance fields
    origin: str = "semantic_compiler"
    provider: str = "unknown_provider"
    rule_name: str | None = None

    # Ranking
    rank: RankSpec | None = None

    # Output columns
    select_exprs: Sequence[SelectExpr] = ()

    # File quality integration
    join_file_quality: bool = True
    file_quality_view: str = "file_quality"


__all__ = [
    "Feature",
    "FeatureKind",
    "HardPredicate",
    "JoinHow",
    "OrderSpec",
    "QualityRelationshipSpec",
    "RankKeep",
    "RankSpec",
    "SelectExpr",
    "SignalsSpec",
]
