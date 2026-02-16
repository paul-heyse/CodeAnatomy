"""Unit tests for semantics.quality dataclasses.

Tests for the three-tier signal model dataclasses including SignalsSpec,
Feature, HardPredicate, RankSpec, and QualityRelationshipSpec.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from semantics.exprs import c, eq, is_not_null, v
from semantics.quality import (
    Feature,
    HardPredicate,
    OrderSpec,
    QualityRelationshipSpec,
    RankSpec,
    SelectExpr,
    SignalsSpec,
)

DEFAULT_BASE_SCORE = 1000.0
DEFAULT_BASE_CONFIDENCE = 0.5
DEFAULT_QUALITY_WEIGHT = 0.0001
CUSTOM_BASE_SCORE = 500.0
CUSTOM_BASE_CONFIDENCE = 0.95
CUSTOM_QUALITY_WEIGHT = 0.001
HARD_PREDICATE_COUNT = 2
FEATURE_WEIGHT = 5.0
CUSTOM_FEATURE_WEIGHT = 10.0
ORDER_BY_COUNT = 2
SELECT_EXPR_COUNT = 3
HIGH_CONFIDENCE = 0.98
TOP_K_ALL = 3


class TestSignalsSpec:
    """Tests for SignalsSpec dataclass."""

    @staticmethod
    def test_default_values() -> None:
        """SignalsSpec has sensible defaults."""
        spec = SignalsSpec()
        assert spec.base_score == DEFAULT_BASE_SCORE
        assert spec.base_confidence == DEFAULT_BASE_CONFIDENCE
        assert spec.hard == ()
        assert spec.features == ()
        assert spec.quality_score_column == "file_quality_score"
        assert spec.quality_weight == DEFAULT_QUALITY_WEIGHT

    @staticmethod
    def test_custom_values() -> None:
        """SignalsSpec accepts custom values."""
        spec = SignalsSpec(
            base_score=CUSTOM_BASE_SCORE,
            base_confidence=CUSTOM_BASE_CONFIDENCE,
            quality_weight=CUSTOM_QUALITY_WEIGHT,
        )
        assert spec.base_score == CUSTOM_BASE_SCORE
        assert spec.base_confidence == CUSTOM_BASE_CONFIDENCE
        assert spec.quality_weight == CUSTOM_QUALITY_WEIGHT

    @staticmethod
    def test_frozen_immutable() -> None:
        """SignalsSpec is immutable (frozen)."""
        spec = SignalsSpec()
        attr_name = "base_score"
        with pytest.raises(FrozenInstanceError):
            setattr(spec, attr_name, 100.0)

    @staticmethod
    def test_with_hard_predicates() -> None:
        """SignalsSpec can contain hard predicates."""
        spec = SignalsSpec(
            hard=[
                HardPredicate(is_not_null("owner_def_id")),
                HardPredicate(eq("owner_def_id", "def_id")),
            ],
        )
        assert len(spec.hard) == HARD_PREDICATE_COUNT

    @staticmethod
    def test_with_features() -> None:
        """SignalsSpec can contain features."""
        from semantics.exprs import case_eq

        spec = SignalsSpec(
            features=[
                Feature("kind_match", case_eq("owner_kind", "def_kind"), weight=FEATURE_WEIGHT),
            ],
        )
        assert len(spec.features) == 1
        assert spec.features[0].name == "kind_match"
        assert spec.features[0].weight == FEATURE_WEIGHT


class TestFeature:
    """Tests for Feature dataclass."""

    @staticmethod
    def test_default_weight() -> None:
        """Feature has default weight of 1.0."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"))
        assert feature.weight == pytest.approx(1.0)
        assert feature.kind == "evidence"

    @staticmethod
    def test_custom_weight() -> None:
        """Feature accepts custom weight."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"), weight=CUSTOM_FEATURE_WEIGHT)
        assert feature.weight == CUSTOM_FEATURE_WEIGHT

    @staticmethod
    def test_quality_kind() -> None:
        """Feature can be quality kind."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"), kind="quality")
        assert feature.kind == "quality"

    @staticmethod
    def test_frozen() -> None:
        """Feature is immutable."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"))
        attr_name = "weight"
        with pytest.raises(FrozenInstanceError):
            setattr(feature, attr_name, 5.0)


class TestHardPredicate:
    """Tests for HardPredicate dataclass."""

    @staticmethod
    def test_contains_predicate() -> None:
        """HardPredicate wraps an ExprSpec predicate."""
        pred = HardPredicate(is_not_null("column"))
        assert pred.predicate is not None

    @staticmethod
    def test_frozen() -> None:
        """HardPredicate is immutable."""
        pred = HardPredicate(is_not_null("column"))
        attr_name = "predicate"
        with pytest.raises(FrozenInstanceError):
            setattr(pred, attr_name, is_not_null("other"))


class TestOrderSpec:
    """Tests for OrderSpec dataclass."""

    @staticmethod
    def test_default_direction() -> None:
        """OrderSpec defaults to descending."""
        order = OrderSpec(c("score"))
        assert order.direction == "desc"

    @staticmethod
    def test_ascending() -> None:
        """OrderSpec can be ascending."""
        order = OrderSpec(c("bstart"), direction="asc")
        assert order.direction == "asc"

    @staticmethod
    def test_frozen() -> None:
        """OrderSpec is immutable."""
        order = OrderSpec(c("score"))
        attr_name = "direction"
        with pytest.raises(FrozenInstanceError):
            setattr(order, attr_name, "asc")


class TestSelectExpr:
    """Tests for SelectExpr dataclass."""

    @staticmethod
    def test_has_expr_and_alias() -> None:
        """SelectExpr has expression and alias."""
        select = SelectExpr(c("entity_id"), "src")
        assert select.alias == "src"

    @staticmethod
    def test_literal_value() -> None:
        """SelectExpr can use literal values."""
        select = SelectExpr(v("has_docstring"), "kind")
        assert select.alias == "kind"

    @staticmethod
    def test_frozen() -> None:
        """SelectExpr is immutable."""
        select = SelectExpr(c("entity_id"), "src")
        attr_name = "alias"
        with pytest.raises(FrozenInstanceError):
            setattr(select, attr_name, "other")


class TestRankSpec:
    """Tests for RankSpec dataclass."""

    @staticmethod
    def test_default_values() -> None:
        """RankSpec has sensible defaults."""
        rank = RankSpec(ambiguity_key_expr=c("entity_id"))
        assert rank.keep == "best"
        assert rank.top_k == 1
        assert rank.order_by == ()
        assert rank.ambiguity_group_id_expr is None

    @staticmethod
    def test_custom_values() -> None:
        """RankSpec accepts custom values."""
        rank = RankSpec(
            ambiguity_key_expr=c("entity_id"),
            ambiguity_group_id_expr=c("entity_id"),
            order_by=[
                OrderSpec(c("score"), direction="desc"),
                OrderSpec(c("bstart"), direction="asc"),
            ],
            keep="all",
            top_k=3,
        )
        assert len(rank.order_by) == ORDER_BY_COUNT
        assert rank.keep == "all"
        assert rank.top_k == TOP_K_ALL

    @staticmethod
    def test_frozen() -> None:
        """RankSpec is immutable."""
        rank = RankSpec(ambiguity_key_expr=c("entity_id"))
        attr_name = "top_k"
        with pytest.raises(FrozenInstanceError):
            setattr(rank, attr_name, 5)


class TestQualityRelationshipSpec:
    """Tests for QualityRelationshipSpec dataclass."""

    @staticmethod
    def test_minimal_spec() -> None:
        """QualityRelationshipSpec requires minimal fields."""
        spec = QualityRelationshipSpec(
            name="test_rel_v1",
            left_view="left_table",
            right_view="right_table",
        )
        assert spec.name == "test_rel_v1"
        assert spec.left_view == "left_table"
        assert spec.right_view == "right_table"
        assert spec.how == "inner"
        assert spec.origin == "semantic_compiler"

    @staticmethod
    def test_full_spec() -> None:
        """QualityRelationshipSpec with all fields."""
        from semantics.exprs import case_eq

        spec = QualityRelationshipSpec(
            name="rel_docstring_owner_v1",
            left_view="cst_docstrings_norm_v1",
            right_view="cst_defs_norm_v1",
            left_on=["file_id"],
            right_on=["file_id"],
            how="inner",
            provider="libcst",
            origin="semantic_compiler",
            rule_name="docstring_owner",
            signals=SignalsSpec(
                base_score=1000,
                base_confidence=HIGH_CONFIDENCE,
                hard=[
                    HardPredicate(is_not_null("l__owner_def_id")),
                    HardPredicate(eq("l__owner_def_id", "r__entity_id")),
                ],
                features=[
                    Feature(
                        "kind_match", case_eq("l__owner_kind", "r__kind"), weight=FEATURE_WEIGHT
                    ),
                ],
            ),
            rank=RankSpec(
                ambiguity_key_expr=c("l__entity_id"),
                order_by=[OrderSpec(c("score"), direction="desc")],
                keep="best",
                top_k=1,
            ),
            select_exprs=[
                SelectExpr(c("l__entity_id"), "src"),
                SelectExpr(c("r__entity_id"), "dst"),
                SelectExpr(v("has_docstring"), "kind"),
            ],
        )
        assert spec.provider == "libcst"
        assert len(spec.signals.hard) == HARD_PREDICATE_COUNT
        assert len(spec.signals.features) == 1
        assert spec.rank is not None
        assert len(spec.select_exprs) == SELECT_EXPR_COUNT

    @staticmethod
    def test_frozen() -> None:
        """QualityRelationshipSpec is immutable."""
        spec = QualityRelationshipSpec(
            name="test_rel_v1",
            left_view="left_table",
            right_view="right_table",
        )
        attr_name = "name"
        with pytest.raises(FrozenInstanceError):
            setattr(spec, attr_name, "other")

    @staticmethod
    def test_default_file_quality_settings() -> None:
        """QualityRelationshipSpec has file quality defaults."""
        spec = QualityRelationshipSpec(
            name="test_rel_v1",
            left_view="left_table",
            right_view="right_table",
        )
        assert spec.join_file_quality is True
        assert spec.file_quality_view == "file_quality"

    @staticmethod
    def test_disabled_file_quality() -> None:
        """QualityRelationshipSpec can disable file quality join."""
        spec = QualityRelationshipSpec(
            name="test_rel_v1",
            left_view="left_table",
            right_view="right_table",
            join_file_quality=False,
        )
        assert spec.join_file_quality is False


class TestQualitySpecsRegistry:
    """Tests for the quality specs registry."""

    @staticmethod
    def test_registry_contains_expected_specs() -> None:
        """QUALITY_RELATIONSHIP_SPECS contains all defined specs."""
        from semantics.quality_specs import QUALITY_RELATIONSHIP_SPECS

        expected_names = [
            "rel_name_symbol",
            "rel_def_symbol",
            "rel_import_symbol",
            "rel_callsite_symbol",
            "rel_cst_docstring_owner_by_id",
            "rel_cst_docstring_owner_by_span",
            "rel_cst_ref_to_scip_symbol",
            "rel_call_to_def_scip",
            "rel_call_to_def_name",
        ]
        for name in expected_names:
            assert name in QUALITY_RELATIONSHIP_SPECS

    @staticmethod
    def test_each_spec_has_required_fields() -> None:
        """Each spec in registry has required fields populated."""
        from semantics.quality_specs import QUALITY_RELATIONSHIP_SPECS

        for name, spec in QUALITY_RELATIONSHIP_SPECS.items():
            assert spec.name == name
            assert spec.left_view
            assert spec.right_view
            assert spec.provider
            assert spec.origin
            assert spec.signals is not None

    @staticmethod
    def test_docstring_owner_by_id_spec() -> None:
        """REL_CST_DOCSTRING_OWNER_BY_ID has correct configuration."""
        from semantics.quality_specs import REL_CST_DOCSTRING_OWNER_BY_ID

        assert REL_CST_DOCSTRING_OWNER_BY_ID.name == "rel_cst_docstring_owner_by_id"
        assert REL_CST_DOCSTRING_OWNER_BY_ID.signals.base_confidence == HIGH_CONFIDENCE
        assert len(REL_CST_DOCSTRING_OWNER_BY_ID.signals.hard) == HARD_PREDICATE_COUNT
        assert REL_CST_DOCSTRING_OWNER_BY_ID.rank is not None
        assert REL_CST_DOCSTRING_OWNER_BY_ID.rank.top_k == 1
