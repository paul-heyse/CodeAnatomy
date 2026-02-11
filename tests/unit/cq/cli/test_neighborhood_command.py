"""Unit tests for neighborhood command integration."""

from __future__ import annotations

import msgspec
from tools.cq.neighborhood.bundle_builder import plan_feasible_slices
from tools.cq.neighborhood.section_layout import materialize_section_layout
from tools.cq.run.spec import _STEP_TAGS, RUN_STEP_TYPES, NeighborhoodStep


def test_neighborhood_step_msgspec_roundtrip() -> None:
    """Test NeighborhoodStep msgspec serialization roundtrip."""
    step = NeighborhoodStep(
        id="test_step",
        target="example.py:10:5",
        lang="python",
        top_k=20,
        no_lsp=True,
    )

    # Serialize and deserialize
    serialized = msgspec.json.encode(step)
    deserialized = msgspec.json.decode(serialized, type=NeighborhoodStep)

    assert deserialized.id == step.id
    assert deserialized.target == step.target
    assert deserialized.lang == step.lang
    assert deserialized.top_k == step.top_k
    assert deserialized.no_lsp == step.no_lsp


def test_plan_feasible_slices_returns_degrades() -> None:
    """Test plan_feasible_slices returns degradation events."""
    requested_slices = ("references", "implementations")
    capabilities: dict[str, object] = {}

    feasible, degrades = plan_feasible_slices(requested_slices, capabilities)

    # Should return empty feasible and degradation events
    assert len(feasible) == 0
    assert len(degrades) == 2
    assert all(d.severity == "info" for d in degrades)
    assert all(d.category == "not_implemented" for d in degrades)


def test_materialize_section_layout_produces_valid_view() -> None:
    """Test materialize_section_layout produces a valid BundleViewV1."""
    from tools.cq.core.snb_schema import (
        BundleMetaV1,
        NeighborhoodGraphSummaryV1,
        SemanticNeighborhoodBundleV1,
        SemanticNodeRefV1,
    )

    # Create a minimal bundle
    subject = SemanticNodeRefV1(
        node_id="test_node",
        kind="function",
        name="test_func",
        display_label="test_func",
        file_path="test.py",
    )

    graph = NeighborhoodGraphSummaryV1(
        node_count=10,
        edge_count=5,
    )

    meta = BundleMetaV1(
        tool="cq",
        workspace_root="/test",
        created_at_ms=1000.0,
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        subject=subject,
        subject_label="test_func",
        meta=meta,
        slices=(),
        graph=graph,
        schema_version="cq.snb.v1",
    )

    # Materialize
    view = materialize_section_layout(bundle)

    # Validate structure
    assert len(view.key_findings) > 0
    assert len(view.sections) > 0

    # Check key findings have target info
    categories = {f.category for f in view.key_findings}
    assert "target" in categories

    # Check sections are in deterministic order
    section_kinds = [s.kind for s in view.sections]
    assert "neighborhood_summary" in section_kinds
    assert "provenance" in section_kinds


def test_neighborhood_step_in_run_step_types() -> None:
    """Test NeighborhoodStep is registered in RUN_STEP_TYPES."""
    assert NeighborhoodStep in RUN_STEP_TYPES


def test_neighborhood_step_in_step_tags() -> None:
    """Test NeighborhoodStep is registered in _STEP_TAGS."""
    assert NeighborhoodStep in _STEP_TAGS
    assert _STEP_TAGS[NeighborhoodStep] == "neighborhood"


def test_neighborhood_step_defaults() -> None:
    """Test NeighborhoodStep default values."""
    step = NeighborhoodStep(target="test.py:10")

    assert step.lang == "python"
    assert step.top_k == 10
    assert step.no_lsp is False


def test_neighborhood_step_with_custom_values() -> None:
    """Test NeighborhoodStep with custom values."""
    step = NeighborhoodStep(
        target="main.rs:50:10",
        lang="rust",
        top_k=5,
        no_lsp=True,
    )

    assert step.target == "main.rs:50:10"
    assert step.lang == "rust"
    assert step.top_k == 5
    assert step.no_lsp is True
