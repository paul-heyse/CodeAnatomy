"""Unit tests for the schema-aware inference phase in the IR pipeline."""

from __future__ import annotations

from semantics.ir import (
    InferredViewProperties,
    SemanticIR,
    SemanticIRView,
)
from semantics.ir_pipeline import infer_semantics
from semantics.view_kinds import ViewKindStr
from tests.test_helpers.immutability import assert_immutable_assignment

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_view(
    name: str,
    kind: ViewKindStr = "normalize",
    inputs: tuple[str, ...] = (),
    outputs: tuple[str, ...] | None = None,
) -> SemanticIRView:
    return SemanticIRView(
        name=name,
        kind=kind,
        inputs=inputs,
        outputs=outputs if outputs is not None else (name,),
    )


def _make_ir(views: tuple[SemanticIRView, ...]) -> SemanticIR:
    return SemanticIR(views=views)


# ---------------------------------------------------------------------------
# Core invariants
# ---------------------------------------------------------------------------


class TestInferSemanticsPreservesStructure:
    """Verify the infer phase is additive and preserves IR structure."""

    def test_same_view_count(self) -> None:
        """Infer must return the same number of views as the input IR."""
        views = (
            _make_view("a"),
            _make_view("b", inputs=("a",)),
            _make_view("c", inputs=("b",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        assert len(result.views) == len(ir.views)

    def test_view_names_preserved(self) -> None:
        """View names must be identical after inference."""
        views = (
            _make_view("x"),
            _make_view("y", inputs=("x",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        original_names = [v.name for v in ir.views]
        inferred_names = [v.name for v in result.views]
        assert inferred_names == original_names

    def test_view_kinds_preserved(self) -> None:
        """View kinds must not be changed by inference."""
        views = (
            _make_view("n", kind="normalize"),
            _make_view("r", kind="relate", inputs=("n", "n")),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        assert result.views[0].kind == "normalize"
        assert result.views[1].kind == "relate"

    def test_view_inputs_outputs_preserved(self) -> None:
        """Inputs and outputs must not be modified by inference."""
        views = (
            _make_view("a", inputs=("x",), outputs=("a_out",)),
            _make_view("b", inputs=("a",), outputs=("b_out",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        assert result.views[0].inputs == ("x",)
        assert result.views[0].outputs == ("a_out",)
        assert result.views[1].inputs == ("a",)
        assert result.views[1].outputs == ("b_out",)

    def test_empty_ir_returns_unchanged(self) -> None:
        """Empty IR should pass through without error."""
        ir = SemanticIR(views=())
        result = infer_semantics(ir)
        assert result.views == ()

    def test_dataset_rows_preserved(self) -> None:
        """Non-view IR fields must be carried through."""
        views = (_make_view("v"),)
        ir = SemanticIR(views=views, model_hash="abc", ir_hash="def")
        result = infer_semantics(ir)
        assert result.model_hash == "abc"
        assert result.ir_hash == "def"


# ---------------------------------------------------------------------------
# Graph position classification
# ---------------------------------------------------------------------------


class TestGraphPositionClassification:
    """Verify graph position is correctly inferred from topology."""

    def test_source_view_has_no_upstream(self) -> None:
        """View with no inputs in the IR is classified as source."""
        views = (
            _make_view("root"),
            _make_view("child", inputs=("root",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        root_props = result.views[0].inferred_properties
        assert root_props is not None
        assert root_props.graph_position == "source"

    def test_terminal_view_has_no_downstream(self) -> None:
        """View with no consumers is classified as terminal."""
        views = (
            _make_view("root"),
            _make_view("leaf", inputs=("root",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        leaf_props = result.views[1].inferred_properties
        assert leaf_props is not None
        assert leaf_props.graph_position == "terminal"

    def test_intermediate_view(self) -> None:
        """View with both upstream and downstream is intermediate."""
        views = (
            _make_view("a"),
            _make_view("b", inputs=("a",)),
            _make_view("c", inputs=("b",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        mid_props = result.views[1].inferred_properties
        assert mid_props is not None
        assert mid_props.graph_position == "intermediate"

    def test_high_fan_out_view(self) -> None:
        """View consumed by 3+ others is classified as high_fan_out."""
        views = (
            _make_view("hub"),
            _make_view("c1", inputs=("hub",)),
            _make_view("c2", inputs=("hub",)),
            _make_view("c3", inputs=("hub",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        hub_props = result.views[0].inferred_properties
        assert hub_props is not None
        assert hub_props.graph_position == "high_fan_out"


# ---------------------------------------------------------------------------
# Cache policy hints
# ---------------------------------------------------------------------------


class TestCachePolicyHints:
    """Verify cache policy hints follow graph position rules."""

    def test_high_fan_out_gets_eager(self) -> None:
        """High fan-out views should get eager cache policy hint."""
        views = (
            _make_view("hub"),
            _make_view("a", inputs=("hub",)),
            _make_view("b", inputs=("hub",)),
            _make_view("c", inputs=("hub",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        hub_props = result.views[0].inferred_properties
        assert hub_props is not None
        assert hub_props.inferred_cache_policy == "eager"

    def test_terminal_gets_lazy(self) -> None:
        """Terminal views should get lazy cache policy hint."""
        views = (
            _make_view("root"),
            _make_view("leaf", inputs=("root",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        leaf_props = result.views[1].inferred_properties
        assert leaf_props is not None
        assert leaf_props.inferred_cache_policy == "lazy"

    def test_source_gets_no_cache_hint(self) -> None:
        """Source views without high fan-out should have no cache hint."""
        views = (
            _make_view("root"),
            _make_view("child", inputs=("root",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        root_props = result.views[0].inferred_properties
        assert root_props is not None
        assert root_props.inferred_cache_policy is None


# ---------------------------------------------------------------------------
# Non-join views have sensible defaults
# ---------------------------------------------------------------------------


class TestNonJoinViewDefaults:
    """Verify non-join views get position/cache but no join metadata."""

    def test_normalize_view_has_no_join_strategy(self) -> None:
        """Normalize views should not get join strategy inference."""
        views = (_make_view("n", kind="normalize"),)
        ir = _make_ir(views)
        result = infer_semantics(ir)
        props = result.views[0].inferred_properties
        assert props is not None
        assert props.inferred_join_strategy is None
        assert props.inferred_join_keys is None

    def test_export_view_has_no_join_strategy(self) -> None:
        """Export views should not get join strategy inference."""
        views = (_make_view("e", kind="export"),)
        ir = _make_ir(views)
        result = infer_semantics(ir)
        props = result.views[0].inferred_properties
        assert props is not None
        assert props.inferred_join_strategy is None

    def test_finalize_view_has_graph_position(self) -> None:
        """Finalize views should still get graph position."""
        views = (
            _make_view("src"),
            _make_view("fin", kind="finalize", inputs=("src",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        fin_props = result.views[1].inferred_properties
        assert fin_props is not None
        assert fin_props.graph_position is not None


# ---------------------------------------------------------------------------
# InferredViewProperties dataclass
# ---------------------------------------------------------------------------


class TestInferredViewPropertiesDefaults:
    """Verify the InferredViewProperties dataclass default behavior."""

    def test_all_none_by_default(self) -> None:
        """All fields should default to None."""
        props = InferredViewProperties()
        assert props.inferred_join_strategy is None
        assert props.inferred_join_keys is None
        assert props.inferred_cache_policy is None
        assert props.graph_position is None

    def test_frozen(self) -> None:
        """InferredViewProperties should be immutable."""
        props = InferredViewProperties(graph_position="source")
        assert_immutable_assignment(
            factory=lambda: props,
            attribute="graph_position",
            attempted_value="terminal",
            expected_exception=AttributeError,
        )


# ---------------------------------------------------------------------------
# Integration: full pipeline
# ---------------------------------------------------------------------------


class TestInferSemanticsFullPipeline:
    """Integration test using build_semantic_ir to verify the infer phase runs."""

    def test_build_semantic_ir_produces_inferred_properties(self) -> None:
        """The full pipeline should produce views with inferred_properties."""
        from semantics.ir_pipeline import build_semantic_ir

        ir = build_semantic_ir()
        # At least some views should have inferred_properties
        views_with_props = [v for v in ir.views if v.inferred_properties is not None]
        assert len(views_with_props) > 0

    def test_relate_views_have_join_metadata(self) -> None:
        """Relate views should have join strategy/keys populated."""
        from semantics.ir_pipeline import build_semantic_ir

        ir = build_semantic_ir()
        relate_views = [v for v in ir.views if v.kind == "relate"]
        assert len(relate_views) > 0
        # At least some relate views should have inferred join strategies
        relate_with_strategy = [
            v
            for v in relate_views
            if v.inferred_properties is not None
            and v.inferred_properties.inferred_join_strategy is not None
        ]
        assert len(relate_with_strategy) > 0

    def test_all_views_have_graph_position(self) -> None:
        """All views should have a graph position assigned."""
        from semantics.ir_pipeline import build_semantic_ir

        ir = build_semantic_ir()
        for view in ir.views:
            assert view.inferred_properties is not None, (
                f"View {view.name!r} has no inferred_properties"
            )
            assert view.inferred_properties.graph_position is not None, (
                f"View {view.name!r} has no graph_position"
            )


# ---------------------------------------------------------------------------
# _resolve_keys_from_inferred
# ---------------------------------------------------------------------------


class TestResolveKeysFromInferred:
    """Verify FILE_IDENTITY key extraction from inferred view properties."""

    def test_returns_file_id_when_present(self) -> None:
        """Extract file_id from inferred join keys."""
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        view = SemanticIRView(
            name="v",
            kind="relate",
            inputs=("left", "right"),
            outputs=("v",),
            inferred_properties=InferredViewProperties(
                inferred_join_keys=(("file_id", "file_id"), ("bstart", "bstart")),
            ),
        )
        result = _resolve_keys_from_inferred(view)
        assert result is not None
        left_on, right_on = result
        assert left_on == ("file_id",)
        assert right_on == ("file_id",)

    def test_returns_file_id_and_path(self) -> None:
        """Extract both file_id and path when present."""
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        view = SemanticIRView(
            name="v",
            kind="relate",
            inputs=("left", "right"),
            outputs=("v",),
            inferred_properties=InferredViewProperties(
                inferred_join_keys=(
                    ("file_id", "file_id"),
                    ("path", "path"),
                    ("symbol", "symbol"),
                ),
            ),
        )
        result = _resolve_keys_from_inferred(view)
        assert result is not None
        resolved_left_on, _resolved_right_on = result
        assert "file_id" in resolved_left_on
        assert "path" in resolved_left_on
        # symbol is not FILE_IDENTITY, should be excluded
        assert "symbol" not in resolved_left_on

    def test_returns_none_when_no_inferred_properties(self) -> None:
        """Return None when inferred_properties is absent."""
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        view = SemanticIRView(
            name="v",
            kind="relate",
            inputs=("left", "right"),
            outputs=("v",),
        )
        assert _resolve_keys_from_inferred(view) is None

    def test_returns_none_when_inferred_keys_is_none(self) -> None:
        """Return None when inferred_join_keys is None."""
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        view = SemanticIRView(
            name="v",
            kind="relate",
            inputs=("left", "right"),
            outputs=("v",),
            inferred_properties=InferredViewProperties(),
        )
        assert _resolve_keys_from_inferred(view) is None

    def test_returns_none_when_only_span_keys(self) -> None:
        """Return None when only non-FILE_IDENTITY keys are inferred."""
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        view = SemanticIRView(
            name="v",
            kind="relate",
            inputs=("left", "right"),
            outputs=("v",),
            inferred_properties=InferredViewProperties(
                inferred_join_keys=(("bstart", "bstart"), ("bend", "bend")),
            ),
        )
        assert _resolve_keys_from_inferred(view) is None

    def test_skips_cross_name_pairs(self) -> None:
        """Cross-name pairs like (file_id, path) are excluded."""
        from semantics.ir_pipeline import _resolve_keys_from_inferred

        view = SemanticIRView(
            name="v",
            kind="relate",
            inputs=("left", "right"),
            outputs=("v",),
            inferred_properties=InferredViewProperties(
                inferred_join_keys=(("file_id", "path"),),
            ),
        )
        assert _resolve_keys_from_inferred(view) is None


# ---------------------------------------------------------------------------
# _build_join_groups with inferred keys
# ---------------------------------------------------------------------------


class TestBuildJoinGroupsWithInferredKeys:
    """Verify _build_join_groups uses inferred keys for empty-key specs."""

    def test_groups_specs_with_inferred_keys(self) -> None:
        """Specs with empty join keys should be grouped via inferred properties."""
        from semantics.ir_pipeline import _build_join_groups
        from semantics.quality import QualityRelationshipSpec

        inferred_props = InferredViewProperties(
            inferred_join_keys=(("file_id", "file_id"),),
        )
        views = [
            SemanticIRView(
                name="rel_a",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_a",),
                inferred_properties=inferred_props,
            ),
            SemanticIRView(
                name="rel_b",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_b",),
                inferred_properties=inferred_props,
            ),
        ]
        specs = {
            "rel_a": QualityRelationshipSpec(
                name="rel_a",
                left_view="left_v",
                right_view="right_v",
                how="inner",
            ),
            "rel_b": QualityRelationshipSpec(
                name="rel_b",
                left_view="left_v",
                right_view="right_v",
                how="inner",
            ),
        }
        groups, _group_views = _build_join_groups(views, specs, existing_names=set())
        # Two specs with same inferred keys -> one join group
        assert len(groups) == 1
        assert set(groups[0].relationship_names) == {"rel_a", "rel_b"}
        assert groups[0].left_on == ("file_id",)
        assert groups[0].right_on == ("file_id",)

    def test_skips_specs_with_no_inferred_keys(self) -> None:
        """Specs without inferred properties should still be skipped."""
        from semantics.ir_pipeline import _build_join_groups
        from semantics.quality import QualityRelationshipSpec

        views = [
            SemanticIRView(
                name="rel_a",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_a",),
                # No inferred_properties
            ),
            SemanticIRView(
                name="rel_b",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_b",),
                # No inferred_properties
            ),
        ]
        specs = {
            "rel_a": QualityRelationshipSpec(
                name="rel_a",
                left_view="left_v",
                right_view="right_v",
                how="inner",
            ),
            "rel_b": QualityRelationshipSpec(
                name="rel_b",
                left_view="left_v",
                right_view="right_v",
                how="inner",
            ),
        }
        groups, _ = _build_join_groups(views, specs, existing_names=set())
        assert len(groups) == 0

    def test_explicit_keys_still_work(self) -> None:
        """Specs with explicit join keys bypass inference as before."""
        from semantics.ir_pipeline import _build_join_groups
        from semantics.quality import QualityRelationshipSpec

        views = [
            SemanticIRView(
                name="rel_a",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_a",),
            ),
            SemanticIRView(
                name="rel_b",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_b",),
            ),
        ]
        specs = {
            "rel_a": QualityRelationshipSpec(
                name="rel_a",
                left_view="left_v",
                right_view="right_v",
                left_on=["file_id"],
                right_on=["file_id"],
                how="inner",
            ),
            "rel_b": QualityRelationshipSpec(
                name="rel_b",
                left_view="left_v",
                right_view="right_v",
                left_on=["file_id"],
                right_on=["file_id"],
                how="inner",
            ),
        }
        groups, _ = _build_join_groups(views, specs, existing_names=set())
        assert len(groups) == 1
        assert groups[0].left_on == ("file_id",)

    def test_mixed_explicit_and_inferred(self) -> None:
        """Explicit and inferred keys resolving to same values group together."""
        from semantics.ir_pipeline import _build_join_groups
        from semantics.quality import QualityRelationshipSpec

        inferred_props = InferredViewProperties(
            inferred_join_keys=(("file_id", "file_id"),),
        )
        views = [
            SemanticIRView(
                name="rel_explicit",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_explicit",),
            ),
            SemanticIRView(
                name="rel_inferred",
                kind="relate",
                inputs=("left_v", "right_v"),
                outputs=("rel_inferred",),
                inferred_properties=inferred_props,
            ),
        ]
        specs = {
            "rel_explicit": QualityRelationshipSpec(
                name="rel_explicit",
                left_view="left_v",
                right_view="right_v",
                left_on=["file_id"],
                right_on=["file_id"],
                how="inner",
            ),
            "rel_inferred": QualityRelationshipSpec(
                name="rel_inferred",
                left_view="left_v",
                right_view="right_v",
                how="inner",
            ),
        }
        groups, _ = _build_join_groups(views, specs, existing_names=set())
        # Both should resolve to the same key and form one group
        assert len(groups) == 1
        assert set(groups[0].relationship_names) == {"rel_explicit", "rel_inferred"}


# ---------------------------------------------------------------------------
# InferenceConfidence on InferredViewProperties
# ---------------------------------------------------------------------------


class TestInferenceConfidenceField:
    """Verify InferenceConfidence is attached to InferredViewProperties."""

    def test_defaults_to_none(self) -> None:
        """The inference_confidence field defaults to None."""
        props = InferredViewProperties()
        assert props.inference_confidence is None

    def test_field_is_accessible(self) -> None:
        """The inference_confidence field can be set at construction."""
        from relspec.inference_confidence import high_confidence

        conf = high_confidence(
            decision_type="join_strategy",
            decision_value="span_overlap",
            evidence_sources=("schema",),
        )
        props = InferredViewProperties(
            graph_position="source",
            inference_confidence=conf,
        )
        assert props.inference_confidence is not None
        assert props.inference_confidence.decision_type == "join_strategy"
        assert props.inference_confidence.confidence_score >= 0.8

    def test_frozen_inference_confidence(self) -> None:
        """InferenceConfidence field is immutable on InferredViewProperties."""
        from relspec.inference_confidence import high_confidence

        conf = high_confidence(
            decision_type="cache_policy",
            decision_value="eager",
            evidence_sources=("graph_topology",),
        )
        props = InferredViewProperties(inference_confidence=conf)
        assert_immutable_assignment(
            factory=lambda: props,
            attribute="inference_confidence",
            attempted_value=None,
            expected_exception=AttributeError,
        )


class TestInferSemanticsAttachesConfidence:
    """Verify that infer_semantics populates inference_confidence."""

    def test_high_fan_out_view_has_cache_confidence(self) -> None:
        """High fan-out views should have cache_policy confidence attached."""
        views = (
            _make_view("hub"),
            _make_view("a", inputs=("hub",)),
            _make_view("b", inputs=("hub",)),
            _make_view("c", inputs=("hub",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        hub_props = result.views[0].inferred_properties
        assert hub_props is not None
        assert hub_props.inference_confidence is not None
        assert hub_props.inference_confidence.decision_type == "cache_policy"
        assert hub_props.inference_confidence.decision_value == "eager"
        assert hub_props.inference_confidence.confidence_score >= 0.8

    def test_terminal_view_has_cache_confidence(self) -> None:
        """Terminal views should have cache_policy confidence attached."""
        views = (
            _make_view("root"),
            _make_view("leaf", inputs=("root",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        leaf_props = result.views[1].inferred_properties
        assert leaf_props is not None
        assert leaf_props.inference_confidence is not None
        assert leaf_props.inference_confidence.decision_type == "cache_policy"
        assert leaf_props.inference_confidence.decision_value == "lazy"

    def test_source_view_with_no_cache_hint_has_no_confidence(self) -> None:
        """Source views without cache hint should have no confidence."""
        views = (
            _make_view("root"),
            _make_view("child", inputs=("root",)),
        )
        ir = _make_ir(views)
        result = infer_semantics(ir)
        root_props = result.views[0].inferred_properties
        assert root_props is not None
        # Source views have no cache hint, so no confidence
        assert root_props.inference_confidence is None

    def test_full_pipeline_relate_views_have_confidence(self) -> None:
        """Relate views in the full pipeline should have confidence metadata."""
        from semantics.ir_pipeline import build_semantic_ir

        ir = build_semantic_ir()
        relate_views = [v for v in ir.views if v.kind == "relate"]
        assert len(relate_views) > 0

        # At least some relate views with inferred strategies should have confidence
        relate_with_confidence = [
            v
            for v in relate_views
            if v.inferred_properties is not None
            and v.inferred_properties.inference_confidence is not None
        ]
        assert len(relate_with_confidence) > 0

    def test_confidence_has_valid_score_range(self) -> None:
        """All confidence scores should be in [0.0, 1.0]."""
        from semantics.ir_pipeline import build_semantic_ir

        ir = build_semantic_ir()
        for view in ir.views:
            if (
                view.inferred_properties is not None
                and view.inferred_properties.inference_confidence is not None
            ):
                score = view.inferred_properties.inference_confidence.confidence_score
                assert 0.0 <= score <= 1.0, (
                    f"View {view.name!r} has out-of-range confidence: {score}"
                )
