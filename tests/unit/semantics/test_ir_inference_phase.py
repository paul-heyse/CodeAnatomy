"""Unit tests for the schema-aware inference phase in the IR pipeline."""

from __future__ import annotations

import pytest

from semantics.ir import (
    InferredViewProperties,
    SemanticIR,
    SemanticIRView,
)
from semantics.ir_pipeline import infer_semantics

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_view(
    name: str,
    kind: str = "normalize",
    inputs: tuple[str, ...] = (),
    outputs: tuple[str, ...] | None = None,
) -> SemanticIRView:
    return SemanticIRView(
        name=name,
        kind=kind,  # type: ignore[arg-type]
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
        with pytest.raises(AttributeError):
            props.graph_position = "terminal"  # type: ignore[misc]


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
