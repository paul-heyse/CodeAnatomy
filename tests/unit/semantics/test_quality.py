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


class TestSignalsSpec:
    """Tests for SignalsSpec dataclass."""

    def test_default_values(self) -> None:
        """SignalsSpec has sensible defaults."""
        spec = SignalsSpec()
        assert spec.base_score == 1000.0
        assert spec.base_confidence == 0.5
        assert spec.hard == ()
        assert spec.features == ()
        assert spec.quality_score_column == "file_quality_score"
        assert spec.quality_weight == 0.0001

    def test_custom_values(self) -> None:
        """SignalsSpec accepts custom values."""
        spec = SignalsSpec(
            base_score=500.0,
            base_confidence=0.95,
            quality_weight=0.001,
        )
        assert spec.base_score == 500.0
        assert spec.base_confidence == 0.95
        assert spec.quality_weight == 0.001

    def test_frozen_immutable(self) -> None:
        """SignalsSpec is immutable (frozen)."""
        spec = SignalsSpec()
        attr_name = "base_score"
        with pytest.raises(FrozenInstanceError):
            setattr(spec, attr_name, 100.0)

    def test_with_hard_predicates(self) -> None:
        """SignalsSpec can contain hard predicates."""
        spec = SignalsSpec(
            hard=[
                HardPredicate(is_not_null("owner_def_id")),
                HardPredicate(eq("owner_def_id", "def_id")),
            ],
        )
        assert len(spec.hard) == 2

    def test_with_features(self) -> None:
        """SignalsSpec can contain features."""
        from semantics.exprs import case_eq

        spec = SignalsSpec(
            features=[
                Feature("kind_match", case_eq("owner_kind", "def_kind"), weight=5.0),
            ],
        )
        assert len(spec.features) == 1
        assert spec.features[0].name == "kind_match"
        assert spec.features[0].weight == 5.0


class TestFeature:
    """Tests for Feature dataclass."""

    def test_default_weight(self) -> None:
        """Feature has default weight of 1.0."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"))
        assert feature.weight == 1.0
        assert feature.kind == "evidence"

    def test_custom_weight(self) -> None:
        """Feature accepts custom weight."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"), weight=10.0)
        assert feature.weight == 10.0

    def test_quality_kind(self) -> None:
        """Feature can be quality kind."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"), kind="quality")
        assert feature.kind == "quality"

    def test_frozen(self) -> None:
        """Feature is immutable."""
        from semantics.exprs import case_eq

        feature = Feature("test", case_eq("a", "b"))
        attr_name = "weight"
        with pytest.raises(FrozenInstanceError):
            setattr(feature, attr_name, 5.0)


class TestHardPredicate:
    """Tests for HardPredicate dataclass."""

    def test_contains_predicate(self) -> None:
        """HardPredicate wraps an ExprSpec predicate."""
        pred = HardPredicate(is_not_null("column"))
        assert pred.predicate is not None

    def test_frozen(self) -> None:
        """HardPredicate is immutable."""
        pred = HardPredicate(is_not_null("column"))
        attr_name = "predicate"
        with pytest.raises(FrozenInstanceError):
            setattr(pred, attr_name, is_not_null("other"))


class TestOrderSpec:
    """Tests for OrderSpec dataclass."""

    def test_default_direction(self) -> None:
        """OrderSpec defaults to descending."""
        order = OrderSpec(c("score"))
        assert order.direction == "desc"

    def test_ascending(self) -> None:
        """OrderSpec can be ascending."""
        order = OrderSpec(c("bstart"), direction="asc")
        assert order.direction == "asc"

    def test_frozen(self) -> None:
        """OrderSpec is immutable."""
        order = OrderSpec(c("score"))
        attr_name = "direction"
        with pytest.raises(FrozenInstanceError):
            setattr(order, attr_name, "asc")


class TestSelectExpr:
    """Tests for SelectExpr dataclass."""

    def test_has_expr_and_alias(self) -> None:
        """SelectExpr has expression and alias."""
        select = SelectExpr(c("entity_id"), "src")
        assert select.alias == "src"

    def test_literal_value(self) -> None:
        """SelectExpr can use literal values."""
        select = SelectExpr(v("has_docstring"), "kind")
        assert select.alias == "kind"

    def test_frozen(self) -> None:
        """SelectExpr is immutable."""
        select = SelectExpr(c("entity_id"), "src")
        attr_name = "alias"
        with pytest.raises(FrozenInstanceError):
            setattr(select, attr_name, "other")


class TestRankSpec:
    """Tests for RankSpec dataclass."""

    def test_default_values(self) -> None:
        """RankSpec has sensible defaults."""
        rank = RankSpec(ambiguity_key_expr=c("entity_id"))
        assert rank.keep == "best"
        assert rank.top_k == 1
        assert rank.order_by == ()
        assert rank.ambiguity_group_id_expr is None

    def test_custom_values(self) -> None:
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
        assert len(rank.order_by) == 2
        assert rank.keep == "all"
        assert rank.top_k == 3

    def test_frozen(self) -> None:
        """RankSpec is immutable."""
        rank = RankSpec(ambiguity_key_expr=c("entity_id"))
        attr_name = "top_k"
        with pytest.raises(FrozenInstanceError):
            setattr(rank, attr_name, 5)


class TestQualityRelationshipSpec:
    """Tests for QualityRelationshipSpec dataclass."""

    def test_minimal_spec(self) -> None:
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

    def test_full_spec(self) -> None:
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
                base_confidence=0.98,
                hard=[
                    HardPredicate(is_not_null("l__owner_def_id")),
                    HardPredicate(eq("l__owner_def_id", "r__entity_id")),
                ],
                features=[
                    Feature("kind_match", case_eq("l__owner_kind", "r__kind"), weight=5.0),
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
        assert len(spec.signals.hard) == 2
        assert len(spec.signals.features) == 1
        assert spec.rank is not None
        assert len(spec.select_exprs) == 3

    def test_frozen(self) -> None:
        """QualityRelationshipSpec is immutable."""
        spec = QualityRelationshipSpec(
            name="test_rel_v1",
            left_view="left_table",
            right_view="right_table",
        )
        attr_name = "name"
        with pytest.raises(FrozenInstanceError):
            setattr(spec, attr_name, "other")

    def test_default_file_quality_settings(self) -> None:
        """QualityRelationshipSpec has file quality defaults."""
        spec = QualityRelationshipSpec(
            name="test_rel_v1",
            left_view="left_table",
            right_view="right_table",
        )
        assert spec.join_file_quality is True
        assert spec.file_quality_view == "file_quality_v1"

    def test_disabled_file_quality(self) -> None:
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

    def test_registry_contains_expected_specs(self) -> None:
        """QUALITY_RELATIONSHIP_SPECS contains all defined specs."""
        from semantics.quality_specs import QUALITY_RELATIONSHIP_SPECS

        expected_names = [
            "rel_name_symbol_v1",
            "rel_def_symbol_v1",
            "rel_import_symbol_v1",
            "rel_callsite_symbol_v1",
            "rel_cst_docstring_owner_by_id_v1",
            "rel_cst_docstring_owner_by_span_v1",
            "rel_cst_ref_to_scip_symbol_v1",
            "rel_call_to_def_scip_v1",
            "rel_call_to_def_name_v1",
        ]
        for name in expected_names:
            assert name in QUALITY_RELATIONSHIP_SPECS

    def test_each_spec_has_required_fields(self) -> None:
        """Each spec in registry has required fields populated."""
        from semantics.quality_specs import QUALITY_RELATIONSHIP_SPECS

        for name, spec in QUALITY_RELATIONSHIP_SPECS.items():
            assert spec.name == name
            assert spec.left_view
            assert spec.right_view
            assert spec.provider
            assert spec.origin
            assert spec.signals is not None

    def test_docstring_owner_by_id_spec(self) -> None:
        """REL_CST_DOCSTRING_OWNER_BY_ID has correct configuration."""
        from semantics.quality_specs import REL_CST_DOCSTRING_OWNER_BY_ID

        assert REL_CST_DOCSTRING_OWNER_BY_ID.name == "rel_cst_docstring_owner_by_id_v1"
        assert REL_CST_DOCSTRING_OWNER_BY_ID.signals.base_confidence == 0.98
        assert len(REL_CST_DOCSTRING_OWNER_BY_ID.signals.hard) == 2
        assert REL_CST_DOCSTRING_OWNER_BY_ID.rank is not None
        assert REL_CST_DOCSTRING_OWNER_BY_ID.rank.top_k == 1
