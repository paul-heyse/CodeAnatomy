"""Tests for deterministic section layout."""

from __future__ import annotations

from tools.cq.core.snb_schema import (
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)
from tools.cq.neighborhood.section_layout import (
    SECTION_ORDER,
    materialize_section_layout,
)


def test_section_order_is_deterministic() -> None:
    """Test that section ordering matches SECTION_ORDER regardless of slice order."""
    # Create bundle with slices in reverse alphabetical order
    slices = (
        NeighborhoodSliceV1(
            kind="siblings",
            title="Siblings",
            total=3,
        ),
        NeighborhoodSliceV1(
            kind="parents",
            title="Parents",
            total=2,
        ),
        NeighborhoodSliceV1(
            kind="children",
            title="Children",
            total=5,
        ),
        NeighborhoodSliceV1(
            kind="callers",
            title="Callers",
            total=1,
        ),
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        slices=slices,
    )

    view = materialize_section_layout(bundle)

    # Synthetic sections to exclude
    synthetic_kinds = {
        "neighborhood_summary",
        "diagnostics",
        "suggested_followups",
        "provenance",
    }

    # Extract slice-backed section kinds (excluding synthetic sections)
    slice_section_kinds = [s.kind for s in view.sections if s.kind not in synthetic_kinds]

    # Expected order from SECTION_ORDER (only those present in bundle)
    expected_slice_kinds = {"parents", "children", "siblings", "callers"}
    expected_kinds = [k for k in SECTION_ORDER if k in expected_slice_kinds]

    assert slice_section_kinds == expected_kinds


def test_unknown_slice_kinds_at_end() -> None:
    """Test that unknown slice kinds appear after known slots."""
    # Create bundle with known and unknown slice kinds
    slices = (
        NeighborhoodSliceV1(
            kind="parents",
            title="Parents",
            total=2,
        ),
        NeighborhoodSliceV1(
            kind="custom_experimental",  # type: ignore[arg-type]
            title="Experimental Slice",
            total=1,
        ),
        NeighborhoodSliceV1(
            kind="children",
            title="Children",
            total=3,
        ),
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        slices=slices,
    )

    view = materialize_section_layout(bundle)

    # Extract all section kinds
    all_kinds = [s.kind for s in view.sections]

    # Known slices should come before unknown
    parents_idx = all_kinds.index("parents")
    children_idx = all_kinds.index("children")
    custom_idx = all_kinds.index("custom_experimental")

    assert parents_idx < custom_idx
    assert children_idx < custom_idx


def test_target_findings_extraction() -> None:
    """Test that target findings are extracted from subject node."""
    subject = SemanticNodeRefV1(
        node_id="test.func.foo",
        kind="function",
        name="foo",
        display_label="foo(x, y)",
        file_path="test.py",
        signature="def foo(x: int, y: str) -> bool",
        qualname="module.foo",
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        subject=subject,
    )

    view = materialize_section_layout(bundle)

    # Check that key findings include target info
    assert len(view.key_findings) > 0

    finding_labels = {f.label for f in view.key_findings}
    assert "Symbol" in finding_labels
    assert "Kind" in finding_labels
    assert "File" in finding_labels
    assert "Signature" in finding_labels
    assert "Qualified Name" in finding_labels


def test_summary_section_includes_stats() -> None:
    """Test that summary section includes graph stats."""
    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        graph=NeighborhoodGraphSummaryV1(
            node_count=42,
            edge_count=108,
        ),
        slices=(
            NeighborhoodSliceV1(
                kind="callers",
                title="Callers",
                total=10,
            ),
            NeighborhoodSliceV1(
                kind="callees",
                title="Callees",
                total=15,
            ),
        ),
    )

    view = materialize_section_layout(bundle)

    # Find summary section
    summary = next((s for s in view.sections if s.kind == "neighborhood_summary"), None)
    assert summary is not None
    assert not summary.collapsed

    # Check items include stats
    items_text = " ".join(summary.items)
    assert "42" in items_text  # node count
    assert "108" in items_text  # edge count
    assert "callers" in items_text.lower()
    assert "callees" in items_text.lower()


def test_diagnostics_section_from_degrade_events() -> None:
    """Test that diagnostics section is built from typed DegradeEventV1."""
    diagnostics = (
        DegradeEventV1(
            stage="semantic.rust",
            severity="error",
            category="timeout",
            message="semantic request timed out after 5s",
        ),
        DegradeEventV1(
            stage="structural.index",
            severity="warning",
            category="partial_coverage",
            message="Interval index missing 3 files",
        ),
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        diagnostics=diagnostics,
    )

    view = materialize_section_layout(bundle)

    # Find diagnostics section
    diag_section = next((s for s in view.sections if s.kind == "diagnostics"), None)
    assert diag_section is not None
    assert diag_section.collapsed

    # Check items include both events
    assert len(diag_section.items) == 2

    items_text = " ".join(diag_section.items)
    assert "semantic.rust" in items_text
    assert "timeout" in items_text
    assert "structural.index" in items_text
    assert "partial_coverage" in items_text


def test_preview_overflow_indicator() -> None:
    """Test that overflow indicator appears when total > preview."""
    slice_ = NeighborhoodSliceV1(
        kind="callers",
        title="Callers",
        total=20,
        preview=(
            SemanticNodeRefV1(
                node_id="caller1",
                kind="function",
                name="caller1",
            ),
            SemanticNodeRefV1(
                node_id="caller2",
                kind="function",
                name="caller2",
            ),
        ),
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        slices=(slice_,),
    )

    view = materialize_section_layout(bundle)

    # Find callers section
    callers = next((s for s in view.sections if s.kind == "callers"), None)
    assert callers is not None

    # Check overflow indicator
    overflow_items = [item for item in callers.items if "more" in item.lower()]
    assert len(overflow_items) == 1
    assert "18 more" in overflow_items[0]


def test_dynamic_collapse_sections() -> None:
    """Test that dynamic collapse works based on item count."""
    # Parents with count <= threshold (3) should not be collapsed
    small_parents = NeighborhoodSliceV1(
        kind="parents",
        title="Parents",
        total=2,
    )

    # Parents with count > threshold should be collapsed
    large_parents = NeighborhoodSliceV1(
        kind="parents",
        title="Parents",
        total=5,
    )

    bundle_small = SemanticNeighborhoodBundleV1(
        bundle_id="test_small",
        slices=(small_parents,),
    )

    bundle_large = SemanticNeighborhoodBundleV1(
        bundle_id="test_large",
        slices=(large_parents,),
    )

    view_small = materialize_section_layout(bundle_small)
    view_large = materialize_section_layout(bundle_large)

    # Find parents sections
    parents_small = next((s for s in view_small.sections if s.kind == "parents"), None)
    parents_large = next((s for s in view_large.sections if s.kind == "parents"), None)

    assert parents_small is not None
    assert parents_large is not None

    assert not parents_small.collapsed
    assert parents_large.collapsed


def test_uncollapsed_sections_never_collapse() -> None:
    """Test that uncollapsed sections never collapse regardless of count."""
    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle",
        subject=SemanticNodeRefV1(
            node_id="test",
            kind="function",
            name="test",
        ),
    )

    view = materialize_section_layout(bundle)

    # Check that summary and followups are not collapsed
    summary = next((s for s in view.sections if s.kind == "neighborhood_summary"), None)
    followups = next((s for s in view.sections if s.kind == "suggested_followups"), None)

    assert summary is not None
    assert followups is not None

    assert not summary.collapsed
    assert not followups.collapsed


def test_provenance_section_includes_metadata() -> None:
    """Test that provenance section includes bundle metadata."""
    meta = BundleMetaV1(
        tool="cq",
        tool_version="1.0.0",
        workspace_root="/workspace",
        created_at_ms=12345.0,
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="test_bundle_123",
        meta=meta,
        schema_version="cq.snb.v1",
    )

    view = materialize_section_layout(bundle)

    # Find provenance section
    prov = next((s for s in view.sections if s.kind == "provenance"), None)
    assert prov is not None

    items_text = " ".join(prov.items)
    assert "cq" in items_text
    assert "1.0.0" in items_text
    assert "/workspace" in items_text
    assert "test_bundle_123" in items_text
    assert "cq.snb.v1" in items_text


def test_empty_bundle_produces_valid_view() -> None:
    """Test that an empty bundle produces a valid (minimal) view."""
    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="empty_bundle",
    )

    view = materialize_section_layout(bundle)

    # Should have at least synthetic sections
    assert len(view.sections) > 0

    # Check for required synthetic sections
    section_kinds = {s.kind for s in view.sections}
    assert "neighborhood_summary" in section_kinds
    assert "suggested_followups" in section_kinds
    assert "provenance" in section_kinds
