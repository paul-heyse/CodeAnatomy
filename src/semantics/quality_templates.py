"""Template factories for common QualityRelationshipSpec patterns.

Provide factory functions that build ``QualityRelationshipSpec`` instances
from minimal parameters, encapsulating shared structural patterns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from semantics.exprs import (
    c,
    case_eq_expr,
    span_contains_expr,
    span_end_expr,
    span_overlaps_expr,
    span_start_expr,
    stable_hash64,
    truthy_expr,
)
from semantics.quality import (
    Feature,
    HardPredicate,
    OrderSpec,
    QualityRelationshipSpec,
    RankSpec,
    SelectExpr,
    SignalsSpec,
)


@dataclass(frozen=True)
class EntitySymbolConfig:
    """Configuration for an entity-to-symbol relationship template.

    Bundle the parameters that vary across entity-to-symbol relationship
    specs while keeping the shared structural pattern in the factory.

    Attributes:
    ----------
    name
        Output name for the relationship view.
    left_view
        Left (entity) view name.
    entity_id_col
        Left-side entity ID column (with ``l__`` prefix, e.g. ``"l__ref_id"``).
    origin
        Origin label for relationship edges.
    span_strategy
        Span geometry: ``"overlap"`` or ``"contains"``.
    scip_role_filter
        Optional SCIP role column to filter on (e.g. ``"r__is_read"``).
        When provided, a ``truthy_expr`` hard predicate is added.
    exact_span_weight
        Weight for the exact span-start match feature.
    """

    name: str
    left_view: str
    entity_id_col: str
    origin: str
    span_strategy: Literal["overlap", "contains"] = "overlap"
    scip_role_filter: str | None = None
    exact_span_weight: float = 20.0


def _span_predicate(strategy: Literal["overlap", "contains"]) -> HardPredicate:
    if strategy == "overlap":
        return HardPredicate(span_overlaps_expr("l__span", "r__span"))
    return HardPredicate(span_contains_expr("l__span", "r__span"))


def entity_symbol_relationship(
    cfg: EntitySymbolConfig,
) -> QualityRelationshipSpec:
    """Build entity-to-symbol relationship spec from configuration.

    Encapsulate the common pattern of joining a CST entity view against
    ``scip_occurrences_norm`` using span geometry, ranking by score, and
    projecting ``entity_id``, ``symbol``, ``path``, ``bstart``, ``bend``.

    Parameters
    ----------
    cfg
        Configuration bundle for the relationship template.

    Returns:
    -------
    QualityRelationshipSpec
        Complete relationship specification.
    """
    hard: list[HardPredicate] = [_span_predicate(cfg.span_strategy)]
    if cfg.scip_role_filter is not None:
        hard.append(HardPredicate(truthy_expr(cfg.scip_role_filter)))

    features: list[Feature] = [
        Feature(
            "exact_span",
            case_eq_expr(span_start_expr("l__span"), span_start_expr("r__span")),
            weight=cfg.exact_span_weight,
        ),
        Feature(
            "exact_end",
            case_eq_expr(span_end_expr("l__span"), span_end_expr("r__span")),
            weight=10.0,
        ),
    ]

    return QualityRelationshipSpec(
        name=cfg.name,
        left_view=cfg.left_view,
        right_view="scip_occurrences_norm",
        how="inner",
        provider="scip",
        origin=cfg.origin,
        rule_name=cfg.name,
        signals=SignalsSpec(
            base_score=2000.0,
            base_confidence=0.95,
            hard=hard,
            features=features,
        ),
        rank=RankSpec(
            ambiguity_key_expr=c(cfg.entity_id_col),
            ambiguity_group_id_expr=stable_hash64(cfg.entity_id_col),
            order_by=[
                OrderSpec(c("score"), direction="desc"),
                OrderSpec(span_start_expr("r__span"), direction="asc"),
            ],
            keep="best",
            top_k=1,
        ),
        select_exprs=[
            SelectExpr(c(cfg.entity_id_col), "entity_id"),
            SelectExpr(c("r__symbol"), "symbol"),
            SelectExpr(c("l__path"), "path"),
            SelectExpr(span_start_expr("l__span"), "bstart"),
            SelectExpr(span_end_expr("l__span"), "bend"),
        ],
    )


__all__ = [
    "EntitySymbolConfig",
    "entity_symbol_relationship",
]
