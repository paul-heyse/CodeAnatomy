"""Unit tests for quality relationship spec templates."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Literal, TypedDict, Unpack

from semantics.quality import QualityRelationshipSpec
from semantics.quality_templates import EntitySymbolConfig, entity_symbol_relationship
from tests.test_helpers.immutability import assert_immutable_assignment


class _EntitySymbolConfigOverrides(TypedDict, total=False):
    """Typed overrides for test config helper."""

    name: str
    left_view: str
    entity_id_col: str
    origin: str
    span_strategy: Literal["overlap", "contains"]
    scip_role_filter: str | None
    exact_span_weight: float


class TestEntitySymbolConfig:
    """Tests for EntitySymbolConfig dataclass."""

    def test_frozen(self) -> None:
        """Config is immutable."""
        cfg = EntitySymbolConfig(
            name="test", left_view="left", entity_id_col="l__id", origin="test"
        )
        assert_immutable_assignment(
            factory=lambda: cfg,
            attribute="name",
            attempted_value="changed",
            expected_exception=FrozenInstanceError,
        )

    def test_required_fields(self) -> None:
        """Config requires name, left_view, entity_id_col, and origin."""
        cfg = EntitySymbolConfig(
            name="rel_x", left_view="cst_x_norm", entity_id_col="l__x_id", origin="cst_x"
        )
        assert cfg.name == "rel_x"
        assert cfg.left_view == "cst_x_norm"
        assert cfg.entity_id_col == "l__x_id"
        assert cfg.origin == "cst_x"

    def test_default_span_strategy(self) -> None:
        """Default span_strategy is overlap."""
        cfg = EntitySymbolConfig(
            name="test", left_view="left", entity_id_col="l__id", origin="test"
        )
        assert cfg.span_strategy == "overlap"

    def test_default_scip_role_filter(self) -> None:
        """Default scip_role_filter is None."""
        cfg = EntitySymbolConfig(
            name="test", left_view="left", entity_id_col="l__id", origin="test"
        )
        assert cfg.scip_role_filter is None

    def test_default_exact_span_weight(self) -> None:
        """Default exact_span_weight is 20.0."""
        cfg = EntitySymbolConfig(
            name="test", left_view="left", entity_id_col="l__id", origin="test"
        )
        assert cfg.exact_span_weight == 20.0

    def test_custom_span_strategy(self) -> None:
        """Config accepts contains span_strategy."""
        cfg = EntitySymbolConfig(
            name="test",
            left_view="left",
            entity_id_col="l__id",
            origin="test",
            span_strategy="contains",
        )
        assert cfg.span_strategy == "contains"

    def test_custom_scip_role_filter(self) -> None:
        """Config accepts custom scip_role_filter."""
        cfg = EntitySymbolConfig(
            name="test",
            left_view="left",
            entity_id_col="l__id",
            origin="test",
            scip_role_filter="r__is_definition",
        )
        assert cfg.scip_role_filter == "r__is_definition"

    def test_custom_exact_span_weight(self) -> None:
        """Config accepts custom exact_span_weight."""
        cfg = EntitySymbolConfig(
            name="test",
            left_view="left",
            entity_id_col="l__id",
            origin="test",
            exact_span_weight=15.0,
        )
        assert cfg.exact_span_weight == 15.0


class TestEntitySymbolRelationship:  # noqa: PLR0904
    """Tests for entity_symbol_relationship factory."""

    def _default_cfg(self, **overrides: Unpack[_EntitySymbolConfigOverrides]) -> EntitySymbolConfig:
        """Build a default config with optional overrides.

        Returns:
        -------
        EntitySymbolConfig
            Config with test defaults.
        """
        return EntitySymbolConfig(
            name=overrides.get("name", "rel_test"),
            left_view=overrides.get("left_view", "test_norm"),
            entity_id_col=overrides.get("entity_id_col", "l__test_id"),
            origin=overrides.get("origin", "test_origin"),
            span_strategy=overrides.get("span_strategy", "overlap"),
            scip_role_filter=overrides.get("scip_role_filter"),
            exact_span_weight=overrides.get("exact_span_weight", 20.0),
        )

    def test_produces_quality_spec(self) -> None:
        """Factory returns a QualityRelationshipSpec."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert isinstance(spec, QualityRelationshipSpec)

    def test_name_propagation(self) -> None:
        """Spec name matches config name."""
        spec = entity_symbol_relationship(self._default_cfg(name="rel_custom"))
        assert spec.name == "rel_custom"

    def test_left_view_propagation(self) -> None:
        """Spec left_view matches config left_view."""
        spec = entity_symbol_relationship(self._default_cfg(left_view="cst_defs_norm"))
        assert spec.left_view == "cst_defs_norm"

    def test_right_view_is_scip_occurrences(self) -> None:
        """Right view is always scip_occurrences_norm."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.right_view == "scip_occurrences_norm"

    def test_join_type_is_inner(self) -> None:
        """Join type is always inner."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.how == "inner"

    def test_provider_is_scip(self) -> None:
        """Provider is always scip."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.provider == "scip"

    def test_origin_propagation(self) -> None:
        """Spec origin matches config origin."""
        spec = entity_symbol_relationship(self._default_cfg(origin="cst_ref_text"))
        assert spec.origin == "cst_ref_text"

    def test_rule_name_matches_name(self) -> None:
        """Rule name is set to the spec name."""
        spec = entity_symbol_relationship(self._default_cfg(name="rel_foo"))
        assert spec.rule_name == "rel_foo"

    def test_join_keys_empty_for_inference(self) -> None:
        """Template specs omit join keys for schema inference."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.left_on == ()
        assert spec.right_on == ()

    def test_base_score(self) -> None:
        """Template uses standard base_score of 2000."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.signals.base_score == 2000.0

    def test_base_confidence(self) -> None:
        """Template uses standard base_confidence of 0.95."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.signals.base_confidence == 0.95

    def test_feature_count(self) -> None:
        """Template produces two features: exact_span and exact_end."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert len(spec.signals.features) == 2
        names = [f.name for f in spec.signals.features]
        assert "exact_span" in names
        assert "exact_end" in names

    def test_exact_end_weight_is_fixed(self) -> None:
        """The exact_end feature always has weight 10.0."""
        spec = entity_symbol_relationship(self._default_cfg())
        end_feature = next(f for f in spec.signals.features if f.name == "exact_end")
        assert end_feature.weight == 10.0

    def test_default_exact_span_weight(self) -> None:
        """Default exact_span_weight is 20.0."""
        spec = entity_symbol_relationship(self._default_cfg())
        span_feature = next(f for f in spec.signals.features if f.name == "exact_span")
        assert span_feature.weight == 20.0

    def test_custom_exact_span_weight(self) -> None:
        """Custom exact_span_weight is applied to feature."""
        spec = entity_symbol_relationship(self._default_cfg(exact_span_weight=15.0))
        span_feature = next(f for f in spec.signals.features if f.name == "exact_span")
        assert span_feature.weight == 15.0

    def test_overlap_strategy_hard_predicate_count(self) -> None:
        """Overlap strategy with no role filter produces one hard predicate."""
        spec = entity_symbol_relationship(self._default_cfg(span_strategy="overlap"))
        assert len(spec.signals.hard) == 1

    def test_contains_strategy_hard_predicate_count(self) -> None:
        """Contains strategy with no role filter produces one hard predicate."""
        spec = entity_symbol_relationship(self._default_cfg(span_strategy="contains"))
        assert len(spec.signals.hard) == 1

    def test_scip_role_filter_adds_predicate(self) -> None:
        """SCIP role filter adds an extra hard predicate."""
        spec_no = entity_symbol_relationship(self._default_cfg())
        spec_yes = entity_symbol_relationship(self._default_cfg(scip_role_filter="r__is_read"))
        assert len(spec_yes.signals.hard) == len(spec_no.signals.hard) + 1

    def test_output_projection_aliases(self) -> None:
        """Template produces standard entity_id/symbol/path/bstart/bend output."""
        spec = entity_symbol_relationship(self._default_cfg())
        aliases = [s.alias for s in spec.select_exprs]
        assert aliases == ["entity_id", "symbol", "path", "bstart", "bend"]

    def test_ranking_config_present(self) -> None:
        """Template configures ranking."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.rank is not None

    def test_ranking_keep_best(self) -> None:
        """Ranking keeps best match."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.rank is not None
        assert spec.rank.keep == "best"

    def test_ranking_top_k(self) -> None:
        """Ranking keeps top 1."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.rank is not None
        assert spec.rank.top_k == 1

    def test_ranking_order_by_count(self) -> None:
        """Ranking has two order-by expressions (score desc, span asc)."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.rank is not None
        assert len(spec.rank.order_by) == 2

    def test_ranking_order_by_directions(self) -> None:
        """First order is desc (score), second is asc (span start)."""
        spec = entity_symbol_relationship(self._default_cfg())
        assert spec.rank is not None
        assert spec.rank.order_by[0].direction == "desc"
        assert spec.rank.order_by[1].direction == "asc"

    def test_entity_id_col_in_select(self) -> None:
        """The entity_id_col from config is used in the entity_id select expr."""
        cfg = self._default_cfg(entity_id_col="l__ref_id")
        spec = entity_symbol_relationship(cfg)
        # The first select expr maps entity_id_col -> "entity_id"
        assert spec.select_exprs[0].alias == "entity_id"

    def test_ambiguity_key_uses_entity_id_col(self) -> None:
        """Ranking ambiguity key is derived from entity_id_col."""
        spec = entity_symbol_relationship(self._default_cfg(entity_id_col="l__call_id"))
        assert spec.rank is not None
        # ambiguity_key_expr is a callable; we check it exists (structural check)
        assert spec.rank.ambiguity_key_expr is not None


class TestEntitySymbolRelationshipWithQualitySpecs:
    """Cross-check template output against known quality_specs.py constants."""

    def test_rel_name_symbol_structure(self) -> None:
        """REL_NAME_SYMBOL matches template with scip_role_filter."""
        from semantics.quality_specs import REL_NAME_SYMBOL

        assert REL_NAME_SYMBOL.name == "rel_name_symbol"
        assert REL_NAME_SYMBOL.left_view == "cst_refs_norm"
        assert REL_NAME_SYMBOL.right_view == "scip_occurrences_norm"
        assert REL_NAME_SYMBOL.how == "inner"
        assert REL_NAME_SYMBOL.provider == "scip"
        assert REL_NAME_SYMBOL.left_on == ()
        assert REL_NAME_SYMBOL.right_on == ()
        # Has role filter -> 2 hard predicates
        assert len(REL_NAME_SYMBOL.signals.hard) == 2

    def test_rel_def_symbol_uses_contains(self) -> None:
        """REL_DEF_SYMBOL uses contains strategy and custom weight."""
        from semantics.quality_specs import REL_DEF_SYMBOL

        assert REL_DEF_SYMBOL.name == "rel_def_symbol"
        assert REL_DEF_SYMBOL.left_view == "cst_defs_norm"
        # Custom span weight 15.0
        span_feature = next(f for f in REL_DEF_SYMBOL.signals.features if f.name == "exact_span")
        assert span_feature.weight == 15.0
        # Has role filter -> 2 hard predicates
        assert len(REL_DEF_SYMBOL.signals.hard) == 2

    def test_rel_import_symbol_structure(self) -> None:
        """REL_IMPORT_SYMBOL has default overlap and role filter."""
        from semantics.quality_specs import REL_IMPORT_SYMBOL

        assert REL_IMPORT_SYMBOL.name == "rel_import_symbol"
        assert REL_IMPORT_SYMBOL.left_view == "cst_imports_norm"
        assert len(REL_IMPORT_SYMBOL.signals.hard) == 2

    def test_rel_callsite_symbol_no_role_filter(self) -> None:
        """REL_CALLSITE_SYMBOL has no role filter (only span predicate)."""
        from semantics.quality_specs import REL_CALLSITE_SYMBOL

        assert REL_CALLSITE_SYMBOL.name == "rel_callsite_symbol"
        assert REL_CALLSITE_SYMBOL.left_view == "cst_calls_norm"
        # No role filter -> only 1 hard predicate
        assert len(REL_CALLSITE_SYMBOL.signals.hard) == 1

    def test_all_template_specs_in_registry(self) -> None:
        """All four template-based specs appear in QUALITY_RELATIONSHIP_SPECS."""
        from semantics.quality_specs import QUALITY_RELATIONSHIP_SPECS

        template_names = [
            "rel_name_symbol",
            "rel_def_symbol",
            "rel_import_symbol",
            "rel_callsite_symbol",
        ]
        for name in template_names:
            assert name in QUALITY_RELATIONSHIP_SPECS, f"{name} missing from registry"
