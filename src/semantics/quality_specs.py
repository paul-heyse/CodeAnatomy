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

>>> spec = QUALITY_RELATIONSHIP_SPECS["rel_cst_docstring_owner_by_id_v1"]
>>> print(spec.signals.base_confidence)
0.98
"""

from __future__ import annotations

from typing import Final

from semantics.exprs import (
    between_overlap,
    c,
    case_eq,
    eq,
    eq_value,
    is_not_null,
    is_null,
    span_contains_span,
    stable_hash64,
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

# -----------------------------------------------------------------------------
# Docstring-to-Owner Relationships
# -----------------------------------------------------------------------------

REL_CST_DOCSTRING_OWNER_BY_ID: Final = QualityRelationshipSpec(
    name="rel_cst_docstring_owner_by_id_v1",
    left_view="cst_docstrings_norm_v1",
    right_view="cst_defs_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
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
            OrderSpec(c("r__bstart"), direction="asc"),
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
    name="rel_cst_docstring_owner_by_span_v1",
    left_view="cst_docstrings_norm_v1",
    right_view="cst_defs_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
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
            Feature("adjacent", case_eq("l__bstart", "r__bend"), weight=10.0),
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

REL_NAME_SYMBOL: Final = QualityRelationshipSpec(
    name="rel_name_symbol_v1",
    left_view="cst_refs_norm_v1",
    right_view="scip_occurrences_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="cst_ref_text",
    rule_name="rel_name_symbol",
    signals=SignalsSpec(
        base_score=2000,
        base_confidence=0.95,
        hard=[
            HardPredicate(between_overlap("l__bstart", "l__bend", "r__bstart", "r__bend")),
            HardPredicate(eq_value("r__is_read", value=True)),
        ],
        features=[
            Feature("exact_span", case_eq("l__bstart", "r__bstart"), weight=20.0),
            Feature("exact_end", case_eq("l__bend", "r__bend"), weight=10.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        ambiguity_group_id_expr=stable_hash64("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("r__bstart"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "entity_id"),
        SelectExpr(c("r__symbol"), "symbol"),
        SelectExpr(c("l__path"), "path"),
        SelectExpr(c("l__bstart"), "bstart"),
        SelectExpr(c("l__bend"), "bend"),
    ],
)

REL_CST_REF_TO_SCIP_SYMBOL: Final = QualityRelationshipSpec(
    name="rel_cst_ref_to_scip_symbol_v1",
    left_view="cst_refs_norm_v1",
    right_view="scip_occurrences_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="semantic_compiler",
    rule_name="cst_ref_to_scip_symbol",
    signals=SignalsSpec(
        base_score=1000,
        base_confidence=0.95,
        hard=[
            # Spans must overlap (byte-aligned)
            HardPredicate(is_not_null("l__bstart")),
            HardPredicate(is_not_null("r__bstart")),
        ],
        features=[
            # Bonus for exact span match
            Feature("exact_span", eq("l__bstart", "r__bstart"), weight=20.0),
            Feature("exact_end", eq("l__bend", "r__bend"), weight=10.0),
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
        SelectExpr(c("l__bstart"), "bstart"),
        SelectExpr(c("l__bend"), "bend"),
    ],
)


# -----------------------------------------------------------------------------
# Call-to-Definition Relationships
# -----------------------------------------------------------------------------

REL_DEF_SYMBOL: Final = QualityRelationshipSpec(
    name="rel_def_symbol_v1",
    left_view="cst_defs_norm_v1",
    right_view="scip_occurrences_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="cst_def_name",
    rule_name="rel_def_symbol",
    signals=SignalsSpec(
        base_score=2000,
        base_confidence=0.95,
        hard=[
            HardPredicate(span_contains_span("l__bstart", "l__bend", "r__bstart", "r__bend")),
            HardPredicate(eq_value("r__is_definition", value=True)),
        ],
        features=[
            Feature("name_span_start", case_eq("l__name_bstart", "r__bstart"), weight=15.0),
            Feature("name_span_end", case_eq("l__name_bend", "r__bend"), weight=10.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        ambiguity_group_id_expr=stable_hash64("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("r__bstart"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "entity_id"),
        SelectExpr(c("r__symbol"), "symbol"),
        SelectExpr(c("l__path"), "path"),
        SelectExpr(c("l__bstart"), "bstart"),
        SelectExpr(c("l__bend"), "bend"),
    ],
)

REL_IMPORT_SYMBOL: Final = QualityRelationshipSpec(
    name="rel_import_symbol_v1",
    left_view="cst_imports_norm_v1",
    right_view="scip_occurrences_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="cst_import_name",
    rule_name="rel_import_symbol",
    signals=SignalsSpec(
        base_score=2000,
        base_confidence=0.95,
        hard=[
            HardPredicate(between_overlap("l__bstart", "l__bend", "r__bstart", "r__bend")),
            HardPredicate(eq_value("r__is_import", value=True)),
        ],
        features=[
            Feature("exact_span", case_eq("l__bstart", "r__bstart"), weight=20.0),
            Feature("exact_end", case_eq("l__bend", "r__bend"), weight=10.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        ambiguity_group_id_expr=stable_hash64("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("r__bstart"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "entity_id"),
        SelectExpr(c("r__symbol"), "symbol"),
        SelectExpr(c("l__path"), "path"),
        SelectExpr(c("l__bstart"), "bstart"),
        SelectExpr(c("l__bend"), "bend"),
    ],
)

REL_CALLSITE_SYMBOL: Final = QualityRelationshipSpec(
    name="rel_callsite_symbol_v1",
    left_view="cst_calls_norm_v1",
    right_view="scip_occurrences_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="cst_callsite",
    rule_name="rel_callsite_symbol",
    signals=SignalsSpec(
        base_score=2000,
        base_confidence=0.95,
        hard=[
            HardPredicate(between_overlap("l__bstart", "l__bend", "r__bstart", "r__bend")),
        ],
        features=[
            Feature("exact_span", case_eq("l__bstart", "r__bstart"), weight=15.0),
            Feature("exact_end", case_eq("l__bend", "r__bend"), weight=10.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("l__entity_id"),
        ambiguity_group_id_expr=stable_hash64("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("r__bstart"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "entity_id"),
        SelectExpr(c("r__symbol"), "symbol"),
        SelectExpr(c("l__path"), "path"),
        SelectExpr(c("l__bstart"), "bstart"),
        SelectExpr(c("l__bend"), "bend"),
    ],
)

REL_CALL_TO_DEF_SCIP: Final = QualityRelationshipSpec(
    name="rel_call_to_def_scip_v1",
    left_view="cst_calls_norm_v1",
    right_view="scip_occurrences_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="semantic_compiler",
    rule_name="call_to_def_scip",
    signals=SignalsSpec(
        base_score=1000,
        base_confidence=0.95,
        hard=[
            HardPredicate(is_not_null("l__bstart")),
            HardPredicate(is_not_null("r__bstart")),
        ],
        features=[
            Feature("exact_span", eq("l__bstart", "r__bstart"), weight=15.0),
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
    ],
)

REL_CALL_TO_DEF_NAME: Final = QualityRelationshipSpec(
    name="rel_call_to_def_name_v1",
    left_view="cst_calls_norm_v1",
    right_view="cst_defs_norm_v1",
    left_on=["file_id"],
    right_on=["file_id"],
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
        ambiguity_key_expr=c("l__entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("r__bstart"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("l__entity_id"), "src"),
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
