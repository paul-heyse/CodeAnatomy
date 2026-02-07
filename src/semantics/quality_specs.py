"""Concrete QualityRelationshipSpec instances for quality-aware relationships.

This module defines declarative specifications for quality-aware relationship
compilation. Each spec uses the three-tier signal model:
- Hard Keys: Required predicates for candidate filtering
- Soft Evidence: Features contributing to score computation
- Quality Signals: File-level quality adjustments

These specs include the core CPG relationships referenced by spec_registry.py
and additional quality-aware relationships used for diagnostics.

Usage
-----
>>> from semantics.quality_specs import QUALITY_RELATIONSHIP_SPECS

>>> spec = QUALITY_RELATIONSHIP_SPECS["rel_cst_docstring_owner_by_id"]
>>> print(spec.signals.base_confidence)
0.98
"""

from __future__ import annotations

from typing import Final

from semantics.exprs import (
    c,
    case_eq,
    case_eq_expr,
    eq,
    eq_expr,
    is_not_null,
    is_not_null_expr,
    is_null,
    span_end_expr,
    span_start_expr,
    v,
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
from semantics.quality_templates import EntitySymbolConfig, entity_symbol_relationship

# -----------------------------------------------------------------------------
# Docstring-to-Owner Relationships
# -----------------------------------------------------------------------------

REL_CST_DOCSTRING_OWNER_BY_ID: Final = QualityRelationshipSpec(
    name="rel_cst_docstring_owner_by_id",
    left_view="cst_docstrings_norm",
    right_view="cst_defs_norm",
    how="inner",
    provider="libcst",
    origin="semantic_compiler",
    rule_name="docstring_owner_by_id",
    signals=SignalsSpec(
        base_score=1000,
        base_confidence=0.98,
        hard=[
            # Must have owner_def_id (ID-based relationship)
            HardPredicate(is_not_null("l__owner_def_id")),
            # Owner ID must match def ID
            HardPredicate(eq("l__owner_def_id", "r__entity_id")),
        ],
        features=[
            # Bonus when owner kind matches def kind
            Feature("kind_match", case_eq("l__owner_kind", "r__kind"), weight=5.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(span_start_expr("r__span"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "src"),
        SelectExpr(c("r__entity_id"), "dst"),
        SelectExpr(v("has_docstring"), "kind"),
    ],
)

REL_CST_DOCSTRING_OWNER_BY_SPAN: Final = QualityRelationshipSpec(
    name="rel_cst_docstring_owner_by_span",
    left_view="cst_docstrings_norm",
    right_view="cst_defs_norm",
    how="inner",
    provider="libcst",
    origin="semantic_compiler",
    rule_name="docstring_owner_by_span",
    signals=SignalsSpec(
        base_score=500,
        base_confidence=0.75,
        hard=[
            # Fallback: no owner_def_id available
            HardPredicate(is_null("l__owner_def_id")),
            # Span proximity: docstring starts immediately after def
        ],
        features=[
            # Bonus for being immediately adjacent
            Feature(
                "adjacent",
                case_eq_expr(span_start_expr("l__span"), span_end_expr("r__span")),
                weight=10.0,
            ),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "src"),
        SelectExpr(c("r__entity_id"), "dst"),
        SelectExpr(v("has_docstring"), "kind"),
    ],
)


# -----------------------------------------------------------------------------
# CST Reference to SCIP Symbol Relationships
# -----------------------------------------------------------------------------

REL_NAME_SYMBOL: Final = entity_symbol_relationship(
    EntitySymbolConfig(
        name="rel_name_symbol",
        left_view="cst_refs_norm",
        entity_id_col="l__ref_id",
        origin="cst_ref_text",
        scip_role_filter="r__is_read",
    ),
)

REL_CST_REF_TO_SCIP_SYMBOL: Final = QualityRelationshipSpec(
    name="rel_cst_ref_to_scip_symbol",
    left_view="cst_refs_norm",
    right_view="scip_occurrences_norm",
    how="inner",
    provider="scip",
    origin="semantic_compiler",
    rule_name="cst_ref_to_scip_symbol",
    signals=SignalsSpec(
        base_score=1000,
        base_confidence=0.95,
        hard=[
            # Spans must overlap (byte-aligned)
            HardPredicate(is_not_null_expr(span_start_expr("l__span"))),
            HardPredicate(is_not_null_expr(span_start_expr("r__span"))),
        ],
        features=[
            # Bonus for exact span match
            Feature(
                "exact_span",
                eq_expr(span_start_expr("l__span"), span_start_expr("r__span")),
                weight=20.0,
            ),
            Feature(
                "exact_end",
                eq_expr(span_end_expr("l__span"), span_end_expr("r__span")),
                weight=10.0,
            ),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "src"),
        SelectExpr(c("r__symbol"), "symbol"),
        SelectExpr(c("l__path"), "path"),
        SelectExpr(span_start_expr("l__span"), "bstart"),
        SelectExpr(span_end_expr("l__span"), "bend"),
    ],
)


# -----------------------------------------------------------------------------
# Call-to-Definition Relationships
# -----------------------------------------------------------------------------

REL_DEF_SYMBOL: Final = entity_symbol_relationship(
    EntitySymbolConfig(
        name="rel_def_symbol",
        left_view="cst_defs_norm",
        entity_id_col="l__def_id",
        origin="cst_def_name",
        span_strategy="contains",
        scip_role_filter="r__is_definition",
        exact_span_weight=15.0,
    ),
)

REL_IMPORT_SYMBOL: Final = entity_symbol_relationship(
    EntitySymbolConfig(
        name="rel_import_symbol",
        left_view="cst_imports_norm",
        entity_id_col="l__import_id",
        origin="cst_import_name",
        scip_role_filter="r__is_import",
    ),
)

REL_CALLSITE_SYMBOL: Final = entity_symbol_relationship(
    EntitySymbolConfig(
        name="rel_callsite_symbol",
        left_view="cst_calls_norm",
        entity_id_col="l__call_id",
        origin="cst_callsite",
        exact_span_weight=15.0,
    ),
)

REL_CALL_TO_DEF_SCIP: Final = QualityRelationshipSpec(
    name="rel_call_to_def_scip",
    left_view="cst_calls_norm",
    right_view="scip_occurrences_norm",
    how="inner",
    provider="scip",
    origin="semantic_compiler",
    rule_name="call_to_def_scip",
    signals=SignalsSpec(
        base_score=1000,
        base_confidence=0.95,
        hard=[
            HardPredicate(is_not_null_expr(span_start_expr("l__span"))),
            HardPredicate(is_not_null_expr(span_start_expr("r__span"))),
        ],
        features=[
            Feature(
                "exact_span",
                eq_expr(span_start_expr("l__span"), span_start_expr("r__span")),
                weight=15.0,
            ),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__call_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__call_id"), "src"),
        SelectExpr(c("r__symbol"), "symbol"),
        SelectExpr(c("l__path"), "path"),
    ],
)

REL_CALL_TO_DEF_NAME: Final = QualityRelationshipSpec(
    name="rel_call_to_def_name",
    left_view="cst_calls_norm",
    right_view="cst_defs_norm",
    how="inner",
    provider="libcst",
    origin="semantic_compiler",
    rule_name="call_to_def_name",
    signals=SignalsSpec(
        base_score=500,
        base_confidence=0.50,
        hard=[
            # Name-based matching (fallback when SCIP unavailable)
            HardPredicate(is_not_null("l__name")),
            HardPredicate(is_not_null("r__name")),
            HardPredicate(eq("l__name", "r__name")),
        ],
        features=[
            # Bonus for same scope
            Feature("same_scope", case_eq("l__scope", "r__scope"), weight=10.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__call_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(span_start_expr("r__span"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__call_id"), "src"),
        SelectExpr(c("r__entity_id"), "dst"),
        SelectExpr(v("calls"), "kind"),
    ],
)


# -----------------------------------------------------------------------------
# Registry of all quality relationship specs
# -----------------------------------------------------------------------------

QUALITY_RELATIONSHIP_SPECS: Final[dict[str, QualityRelationshipSpec]] = {
    spec.name: spec
    for spec in [
        REL_NAME_SYMBOL,
        REL_DEF_SYMBOL,
        REL_IMPORT_SYMBOL,
        REL_CALLSITE_SYMBOL,
        REL_CST_DOCSTRING_OWNER_BY_ID,
        REL_CST_DOCSTRING_OWNER_BY_SPAN,
        REL_CST_REF_TO_SCIP_SYMBOL,
        REL_CALL_TO_DEF_SCIP,
        REL_CALL_TO_DEF_NAME,
    ]
}


__all__ = [
    "QUALITY_RELATIONSHIP_SPECS",
    "REL_CALLSITE_SYMBOL",
    "REL_CALL_TO_DEF_NAME",
    "REL_CALL_TO_DEF_SCIP",
    "REL_CST_DOCSTRING_OWNER_BY_ID",
    "REL_CST_DOCSTRING_OWNER_BY_SPAN",
    "REL_CST_REF_TO_SCIP_SYMBOL",
    "REL_DEF_SYMBOL",
    "REL_IMPORT_SYMBOL",
    "REL_NAME_SYMBOL",
]
