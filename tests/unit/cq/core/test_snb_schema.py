"""Unit tests for SNB schema structures."""

from __future__ import annotations

import pytest
from tools.cq.core.snb_schema import (
    ArtifactPointerV1,
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceKind,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNeighborhoodBundleRefV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)


def test_artifact_pointer_v1_construction() -> None:
    """Test ArtifactPointerV1 construction with required fields."""
    pointer = ArtifactPointerV1(
        artifact_kind="snb.bundle",
        artifact_id="bundle-123",
        deterministic_id="sha256-abc",
    )
    assert pointer.artifact_kind == "snb.bundle"
    assert pointer.artifact_id == "bundle-123"
    assert pointer.deterministic_id == "sha256-abc"
    assert pointer.byte_size == 0
    assert pointer.storage_path is None
    assert pointer.metadata is None


def test_degrade_event_v1_construction() -> None:
    """Test DegradeEventV1 construction with required fields."""
    event = DegradeEventV1(stage="semantic.rust")
    assert event.stage == "semantic.rust"
    assert event.severity == "warning"
    assert event.category == ""
    assert event.message == ""
    assert event.correlation_key is None


def test_degrade_event_v1_severity_values() -> None:
    """Test DegradeEventV1 accepts valid severity values."""
    info = DegradeEventV1(stage="test", severity="info")
    warning = DegradeEventV1(stage="test", severity="warning")
    error = DegradeEventV1(stage="test", severity="error")

    assert info.severity == "info"
    assert warning.severity == "warning"
    assert error.severity == "error"


def test_semantic_node_ref_v1_construction() -> None:
    """Test SemanticNodeRefV1 construction with required fields."""
    node = SemanticNodeRefV1(
        node_id="node-123",
        kind="function",
        name="build_graph",
    )
    assert node.node_id == "node-123"
    assert node.kind == "function"
    assert node.name == "build_graph"
    assert node.display_label == ""
    assert node.file_path == ""
    assert node.byte_span is None
    assert node.signature is None
    assert node.qualname is None


def test_semantic_node_ref_v1_with_all_fields() -> None:
    """Test SemanticNodeRefV1 with all fields."""
    node = SemanticNodeRefV1(
        node_id="node-123",
        kind="function",
        name="build_graph",
        display_label="build_graph()",
        file_path="src/graph.py",
        byte_span=(100, 200),
        signature="def build_graph(req: Request) -> Result",
        qualname="src.graph.build_graph",
    )
    assert node.byte_span == (100, 200)
    assert node.signature == "def build_graph(req: Request) -> Result"
    assert node.qualname == "src.graph.build_graph"


def test_semantic_edge_v1_construction() -> None:
    """Test SemanticEdgeV1 construction with required fields."""
    edge = SemanticEdgeV1(
        edge_id="edge-123",
        source_node_id="node-1",
        target_node_id="node-2",
        edge_kind="calls",
    )
    assert edge.edge_id == "edge-123"
    assert edge.source_node_id == "node-1"
    assert edge.target_node_id == "node-2"
    assert edge.edge_kind == "calls"
    assert edge.weight == 1.0
    assert edge.evidence_source == ""
    assert edge.metadata is None


def test_neighborhood_slice_kind_contains_all_values() -> None:
    """Test NeighborhoodSliceKind contains all 13 expected values."""
    expected_kinds: set[NeighborhoodSliceKind] = {
        "callers",
        "callees",
        "references",
        "implementations",
        "type_supertypes",
        "type_subtypes",
        "parents",
        "children",
        "siblings",
        "enclosing_context",
        "imports",
        "importers",
        "related",
    }
    # NeighborhoodSliceKind is a Literal type, so we can't iterate it directly
    # but we can test construction with each value
    for kind in expected_kinds:
        slice_v1 = NeighborhoodSliceV1(kind=kind, title=f"{kind} slice")
        assert slice_v1.kind == kind


def test_neighborhood_slice_v1_construction() -> None:
    """Test NeighborhoodSliceV1 construction with required fields."""
    slice_v1 = NeighborhoodSliceV1(
        kind="callers",
        title="Callers",
    )
    assert slice_v1.kind == "callers"
    assert slice_v1.title == "Callers"
    assert slice_v1.total == 0
    assert slice_v1.preview == ()
    assert slice_v1.edges == ()
    assert slice_v1.collapsed is True
    assert slice_v1.metadata is None


def test_neighborhood_slice_v1_with_preview() -> None:
    """Test NeighborhoodSliceV1 with preview nodes."""
    node1 = SemanticNodeRefV1(node_id="n1", kind="function", name="caller1")
    node2 = SemanticNodeRefV1(node_id="n2", kind="function", name="caller2")

    slice_v1 = NeighborhoodSliceV1(
        kind="callers",
        title="Callers",
        total=10,
        preview=(node1, node2),
    )
    assert slice_v1.total == 10
    assert len(slice_v1.preview) == 2
    assert slice_v1.preview[0].name == "caller1"
    assert slice_v1.preview[1].name == "caller2"


def test_semantic_neighborhood_bundle_ref_v1_construction() -> None:
    """Test SemanticNeighborhoodBundleRefV1 construction."""
    bundle_ref = SemanticNeighborhoodBundleRefV1(
        bundle_id="bundle-123",
        deterministic_id="sha256-xyz",
    )
    assert bundle_ref.bundle_id == "bundle-123"
    assert bundle_ref.deterministic_id == "sha256-xyz"
    assert bundle_ref.byte_size == 0
    assert bundle_ref.artifact_path is None
    assert bundle_ref.preview_slices == ()
    assert bundle_ref.subject_node_id == ""
    assert bundle_ref.subject_label == ""


def test_bundle_meta_v1_construction() -> None:
    """Test BundleMetaV1 construction with defaults."""
    meta = BundleMetaV1()
    assert meta.tool == "cq"
    assert meta.tool_version is None
    assert meta.workspace_root is None
    assert meta.query_text is None
    assert meta.created_at_ms is None
    assert meta.semantic_sources == ()
    assert meta.limits is None


def test_bundle_meta_v1_with_values() -> None:
    """Test BundleMetaV1 with all fields populated."""
    meta = BundleMetaV1(
        tool="cq",
        tool_version="0.1.0",
        workspace_root="/workspace",
        query_text="entity=function name=build_graph",
        created_at_ms=1234567890.0,
        semantic_sources=({"name": "tree_sitter", "version": "0.25.10"},),
        limits={"max_nodes": 1000, "max_depth": 3},
    )
    assert meta.tool_version == "0.1.0"
    assert meta.workspace_root == "/workspace"
    assert meta.query_text == "entity=function name=build_graph"
    assert meta.created_at_ms == 1234567890.0
    assert len(meta.semantic_sources) == 1
    assert meta.limits == {"max_nodes": 1000, "max_depth": 3}


def test_neighborhood_graph_summary_v1_construction() -> None:
    """Test NeighborhoodGraphSummaryV1 construction."""
    summary = NeighborhoodGraphSummaryV1()
    assert summary.node_count == 0
    assert summary.edge_count == 0
    assert summary.full_graph_artifact is None


def test_neighborhood_graph_summary_v1_with_counts() -> None:
    """Test NeighborhoodGraphSummaryV1 with counts and artifact."""
    artifact = ArtifactPointerV1(
        artifact_kind="graph",
        artifact_id="graph-123",
        deterministic_id="sha256-graph",
    )
    summary = NeighborhoodGraphSummaryV1(
        node_count=42,
        edge_count=100,
        full_graph_artifact=artifact,
    )
    assert summary.node_count == 42
    assert summary.edge_count == 100
    assert summary.full_graph_artifact is not None
    assert summary.full_graph_artifact.artifact_id == "graph-123"


def test_semantic_neighborhood_bundle_v1_canonical_fields() -> None:
    """Test SemanticNeighborhoodBundleV1 canonical schema fields."""
    bundle = SemanticNeighborhoodBundleV1(bundle_id="bundle-123")

    # Canonical fields must exist
    assert bundle.bundle_id == "bundle-123"
    assert bundle.subject is None
    assert bundle.subject_label == ""
    assert bundle.meta is None
    assert bundle.slices == ()
    assert bundle.graph is None
    assert bundle.node_index is None
    assert bundle.artifacts == ()
    assert bundle.diagnostics == ()
    assert bundle.schema_version == "cq.snb.v1"


def test_semantic_neighborhood_bundle_v1_with_full_data() -> None:
    """Test SemanticNeighborhoodBundleV1 with complete data."""
    subject_node = SemanticNodeRefV1(
        node_id="subject-1",
        kind="function",
        name="build_graph",
        display_label="build_graph()",
    )

    meta = BundleMetaV1(
        tool="cq",
        tool_version="0.1.0",
        workspace_root="/workspace",
    )

    slice_v1 = NeighborhoodSliceV1(
        kind="callers",
        title="Callers",
        total=5,
    )

    graph = NeighborhoodGraphSummaryV1(node_count=10, edge_count=20)

    node_index = {
        "node-1": SemanticNodeRefV1(node_id="node-1", kind="function", name="caller1"),
        "node-2": SemanticNodeRefV1(node_id="node-2", kind="function", name="caller2"),
    }

    artifact = ArtifactPointerV1(
        artifact_kind="semantic.call_graph",
        artifact_id="artifact-1",
        deterministic_id="sha256-artifact",
    )

    diagnostic = DegradeEventV1(
        stage="semantic.rust",
        severity="warning",
        category="timeout",
        message="semantic server timeout",
    )

    bundle = SemanticNeighborhoodBundleV1(
        bundle_id="bundle-123",
        subject=subject_node,
        subject_label="build_graph()",
        meta=meta,
        slices=(slice_v1,),
        graph=graph,
        node_index=node_index,
        artifacts=(artifact,),
        diagnostics=(diagnostic,),
    )

    # Verify all fields
    assert bundle.subject is not None
    assert bundle.subject.name == "build_graph"
    assert bundle.subject_label == "build_graph()"
    assert bundle.meta is not None
    assert bundle.meta.tool_version == "0.1.0"
    assert len(bundle.slices) == 1
    assert bundle.slices[0].kind == "callers"
    assert bundle.graph is not None
    assert bundle.graph.node_count == 10
    assert bundle.node_index is not None
    assert len(bundle.node_index) == 2
    assert len(bundle.artifacts) == 1
    assert bundle.artifacts[0].artifact_kind == "semantic.call_graph"
    assert len(bundle.diagnostics) == 1
    assert bundle.diagnostics[0].stage == "semantic.rust"
    assert bundle.schema_version == "cq.snb.v1"


@pytest.mark.parametrize(
    "kind",
    [
        "callers",
        "callees",
        "references",
        "implementations",
        "type_supertypes",
        "type_subtypes",
        "parents",
        "children",
        "siblings",
        "enclosing_context",
        "imports",
        "importers",
        "related",
    ],
)
def test_all_neighborhood_slice_kinds(kind: NeighborhoodSliceKind) -> None:
    """Test that all 13 NeighborhoodSliceKind values are valid."""
    slice_v1 = NeighborhoodSliceV1(kind=kind, title=f"{kind} test")
    assert slice_v1.kind == kind
