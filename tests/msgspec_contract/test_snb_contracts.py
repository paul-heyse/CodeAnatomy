"""msgspec contract roundtrip tests for SNB schema."""

from __future__ import annotations

import msgspec
import pytest
from tools.cq.core.snb_registry import (
    DETAILS_KIND_REGISTRY,
    decode_finding_details,
    resolve_kind,
    validate_finding_details,
)
from tools.cq.core.snb_schema import (
    ArtifactPointerV1,
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNeighborhoodBundleRefV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)

PREVIEW_NODE_COUNT = 2
BUNDLE_GRAPH_NODE_COUNT = 10


def test_artifact_pointer_v1_roundtrip() -> None:
    """Test ArtifactPointerV1 msgspec encode/decode roundtrip."""
    original = ArtifactPointerV1(
        artifact_kind="snb.bundle",
        artifact_id="bundle-123",
        deterministic_id="sha256-abc",
        byte_size=1024,
        storage_path="/path/to/artifact",
        metadata={"key": "value"},
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=ArtifactPointerV1)

    assert decoded.artifact_kind == original.artifact_kind
    assert decoded.artifact_id == original.artifact_id
    assert decoded.deterministic_id == original.deterministic_id
    assert decoded.byte_size == original.byte_size
    assert decoded.storage_path == original.storage_path
    assert decoded.metadata == original.metadata


def test_degrade_event_v1_roundtrip() -> None:
    """Test DegradeEventV1 msgspec encode/decode roundtrip."""
    original = DegradeEventV1(
        stage="lsp.rust",
        severity="error",
        category="timeout",
        message="LSP server timeout after 30s",
        correlation_key="rust-analyzer-timeout",
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=DegradeEventV1)

    assert decoded.stage == original.stage
    assert decoded.severity == original.severity
    assert decoded.category == original.category
    assert decoded.message == original.message
    assert decoded.correlation_key == original.correlation_key


def test_semantic_node_ref_v1_roundtrip() -> None:
    """Test SemanticNodeRefV1 msgspec encode/decode roundtrip."""
    original = SemanticNodeRefV1(
        node_id="node-123",
        kind="function",
        name="build_graph",
        display_label="build_graph()",
        file_path="src/graph.py",
        byte_span=(100, 200),
        signature="def build_graph(req: Request) -> Result",
        qualname="src.graph.build_graph",
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=SemanticNodeRefV1)

    assert decoded.node_id == original.node_id
    assert decoded.kind == original.kind
    assert decoded.name == original.name
    assert decoded.display_label == original.display_label
    assert decoded.file_path == original.file_path
    assert decoded.byte_span == original.byte_span
    assert decoded.signature == original.signature
    assert decoded.qualname == original.qualname


def test_semantic_edge_v1_roundtrip() -> None:
    """Test SemanticEdgeV1 msgspec encode/decode roundtrip."""
    original = SemanticEdgeV1(
        edge_id="edge-123",
        source_node_id="node-1",
        target_node_id="node-2",
        edge_kind="calls",
        weight=0.95,
        evidence_source="ast",
        metadata={"confidence": 0.95, "line_number": 42},
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=SemanticEdgeV1)

    assert decoded.edge_id == original.edge_id
    assert decoded.source_node_id == original.source_node_id
    assert decoded.target_node_id == original.target_node_id
    assert decoded.edge_kind == original.edge_kind
    assert decoded.weight == original.weight
    assert decoded.evidence_source == original.evidence_source
    assert decoded.metadata == original.metadata


def test_neighborhood_slice_v1_roundtrip() -> None:
    """Test NeighborhoodSliceV1 msgspec encode/decode roundtrip."""
    node1 = SemanticNodeRefV1(node_id="n1", kind="function", name="caller1")
    node2 = SemanticNodeRefV1(node_id="n2", kind="function", name="caller2")
    edge1 = SemanticEdgeV1(
        edge_id="e1",
        source_node_id="n1",
        target_node_id="subject",
        edge_kind="calls",
    )

    original = NeighborhoodSliceV1(
        kind="callers",
        title="Callers",
        total=10,
        preview=(node1, node2),
        edges=(edge1,),
        collapsed=False,
        metadata={"source": "lsp"},
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=NeighborhoodSliceV1)

    assert decoded.kind == original.kind
    assert decoded.title == original.title
    assert decoded.total == original.total
    assert len(decoded.preview) == PREVIEW_NODE_COUNT
    assert decoded.preview[0].name == "caller1"
    assert decoded.preview[1].name == "caller2"
    assert len(decoded.edges) == 1
    assert decoded.edges[0].edge_id == "e1"
    assert decoded.collapsed is False
    assert decoded.metadata == original.metadata


def test_semantic_neighborhood_bundle_ref_v1_roundtrip() -> None:
    """Test SemanticNeighborhoodBundleRefV1 msgspec encode/decode roundtrip."""
    original = SemanticNeighborhoodBundleRefV1(
        bundle_id="bundle-123",
        deterministic_id="sha256-xyz",
        byte_size=2048,
        artifact_path="/artifacts/bundle-123.json",
        preview_slices=("callers", "callees", "imports"),
        subject_node_id="subject-1",
        subject_label="build_graph()",
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=SemanticNeighborhoodBundleRefV1)

    assert decoded.bundle_id == original.bundle_id
    assert decoded.deterministic_id == original.deterministic_id
    assert decoded.byte_size == original.byte_size
    assert decoded.artifact_path == original.artifact_path
    assert decoded.preview_slices == original.preview_slices
    assert decoded.subject_node_id == original.subject_node_id
    assert decoded.subject_label == original.subject_label


def test_bundle_meta_v1_roundtrip() -> None:
    """Test BundleMetaV1 msgspec encode/decode roundtrip."""
    original = BundleMetaV1(
        tool="cq",
        tool_version="0.1.0",
        workspace_root="/workspace",
        query_text="entity=function name=build_graph",
        created_at_ms=1234567890.0,
        semantic_sources=(
            {"name": "tree_sitter", "version": "0.25.10"},
            {"name": "libcst", "version": "1.8.5"},
            {"name": "ast_grep", "version": "0.40.5"},
        ),
        limits={"max_nodes": 1000, "max_depth": 3},
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=BundleMetaV1)

    assert decoded.tool == original.tool
    assert decoded.tool_version == original.tool_version
    assert decoded.workspace_root == original.workspace_root
    assert decoded.query_text == original.query_text
    assert decoded.created_at_ms == original.created_at_ms
    assert decoded.semantic_sources == original.semantic_sources
    assert decoded.limits == original.limits


def test_neighborhood_graph_summary_v1_roundtrip() -> None:
    """Test NeighborhoodGraphSummaryV1 msgspec encode/decode roundtrip."""
    artifact = ArtifactPointerV1(
        artifact_kind="graph",
        artifact_id="graph-123",
        deterministic_id="sha256-graph",
    )

    original = NeighborhoodGraphSummaryV1(
        node_count=42,
        edge_count=100,
        full_graph_artifact=artifact,
    )

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=NeighborhoodGraphSummaryV1)

    assert decoded.node_count == original.node_count
    assert decoded.edge_count == original.edge_count
    assert decoded.full_graph_artifact is not None
    assert decoded.full_graph_artifact.artifact_id == artifact.artifact_id


def test_semantic_neighborhood_bundle_v1_roundtrip() -> None:
    """Test SemanticNeighborhoodBundleV1 msgspec encode/decode roundtrip."""
    subject_node = SemanticNodeRefV1(
        node_id="subject-1",
        kind="function",
        name="build_graph",
    )

    meta = BundleMetaV1(tool="cq", tool_version="0.1.0")

    slice_v1 = NeighborhoodSliceV1(kind="callers", title="Callers", total=5)

    graph = NeighborhoodGraphSummaryV1(node_count=10, edge_count=20)

    node_index = {
        "node-1": SemanticNodeRefV1(node_id="node-1", kind="function", name="caller1"),
    }

    artifact = ArtifactPointerV1(
        artifact_kind="lsp.call_graph",
        artifact_id="artifact-1",
        deterministic_id="sha256-artifact",
    )

    diagnostic = DegradeEventV1(
        stage="lsp.rust",
        severity="warning",
        message="LSP timeout",
    )

    original = SemanticNeighborhoodBundleV1(
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

    encoded = msgspec.json.encode(original)
    decoded = msgspec.json.decode(encoded, type=SemanticNeighborhoodBundleV1)

    assert decoded.bundle_id == original.bundle_id
    assert decoded.subject is not None
    assert decoded.subject.name == "build_graph"
    assert decoded.subject_label == "build_graph()"
    assert decoded.meta is not None
    assert decoded.meta.tool_version == "0.1.0"
    assert len(decoded.slices) == 1
    assert decoded.graph is not None
    assert decoded.graph.node_count == BUNDLE_GRAPH_NODE_COUNT
    assert decoded.node_index is not None
    assert len(decoded.node_index) == 1
    assert len(decoded.artifacts) == 1
    assert len(decoded.diagnostics) == 1
    assert decoded.schema_version == "cq.snb.v1"


def test_backward_compatibility_extra_fields() -> None:
    """Test that extra fields are tolerated for backward compatibility."""
    # Encode with extra fields
    data = {
        "bundle_id": "bundle-123",
        "schema_version": "cq.snb.v1",
        "extra_field": "should be ignored",
        "another_field": 42,
    }

    encoded = msgspec.json.encode(data)
    decoded = msgspec.json.decode(encoded, type=SemanticNeighborhoodBundleV1)

    assert decoded.bundle_id == "bundle-123"
    assert decoded.schema_version == "cq.snb.v1"


def test_registry_resolve_kind_known() -> None:
    """Test registry resolve_kind returns correct types for known kinds."""
    assert resolve_kind("cq.snb.bundle.v1") is SemanticNeighborhoodBundleV1
    assert resolve_kind("cq.snb.bundle_ref.v1") is SemanticNeighborhoodBundleRefV1
    assert resolve_kind("cq.snb.node.v1") is SemanticNodeRefV1
    assert resolve_kind("cq.snb.edge.v1") is SemanticEdgeV1
    assert resolve_kind("cq.snb.slice.v1") is NeighborhoodSliceV1
    assert resolve_kind("cq.snb.degrade.v1") is DegradeEventV1
    assert resolve_kind("cq.snb.bundle_meta.v1") is BundleMetaV1
    assert resolve_kind("cq.snb.graph_summary.v1") is NeighborhoodGraphSummaryV1


def test_registry_resolve_kind_unknown() -> None:
    """Test registry resolve_kind returns None for unknown kinds."""
    assert resolve_kind("unknown.kind.v1") is None
    assert resolve_kind("") is None
    assert resolve_kind("cq.snb.invalid.v1") is None


def test_validate_finding_details_success() -> None:
    """Test validate_finding_details succeeds for valid details."""
    details: dict[str, object] = {
        "kind": "cq.snb.degrade.v1",
        "stage": "lsp.rust",
        "severity": "warning",
    }
    assert validate_finding_details(details) is True


def test_validate_finding_details_unknown_kind() -> None:
    """Test validate_finding_details fails for unknown kind."""
    details: dict[str, object] = {
        "kind": "unknown.kind.v1",
        "data": "value",
    }
    assert validate_finding_details(details) is False


def test_validate_finding_details_missing_kind() -> None:
    """Test validate_finding_details fails when kind is missing."""
    details: dict[str, object] = {"data": "value"}
    assert validate_finding_details(details) is False


def test_validate_finding_details_non_string_kind() -> None:
    """Test validate_finding_details fails when kind is not a string."""
    details: dict[str, object] = {"kind": 123, "data": "value"}
    assert validate_finding_details(details) is False


def test_decode_finding_details_success() -> None:
    """Test decode_finding_details succeeds for valid details."""
    details: dict[str, object] = {
        "kind": "cq.snb.degrade.v1",
        "stage": "lsp.rust",
        "severity": "warning",
        "category": "timeout",
        "message": "LSP timeout",
    }

    result = decode_finding_details(details)
    assert result is not None
    assert isinstance(result, DegradeEventV1)
    assert result.stage == "lsp.rust"
    assert result.severity == "warning"
    assert result.category == "timeout"
    assert result.message == "LSP timeout"


def test_decode_finding_details_unknown_kind() -> None:
    """Test decode_finding_details returns None for unknown kind."""
    details: dict[str, object] = {
        "kind": "unknown.kind.v1",
        "data": "value",
    }
    assert decode_finding_details(details) is None


def test_decode_finding_details_missing_kind() -> None:
    """Test decode_finding_details returns None when kind is missing."""
    details: dict[str, object] = {"data": "value"}
    assert decode_finding_details(details) is None


def test_decode_finding_details_invalid_data() -> None:
    """Test decode_finding_details returns None for invalid data."""
    details: dict[str, object] = {
        "kind": "cq.snb.degrade.v1",
        # Missing required 'stage' field
        "severity": "warning",
    }
    assert decode_finding_details(details) is None


def test_decode_finding_details_type_mismatch() -> None:
    """Test decode_finding_details returns None for type mismatch."""
    details: dict[str, object] = {
        "kind": "cq.snb.degrade.v1",
        "stage": 123,  # Should be string
        "severity": "warning",
    }
    assert decode_finding_details(details) is None


def test_registry_contains_all_types() -> None:
    """Test that registry contains all 8 SNB types."""
    expected_kinds = {
        "cq.snb.bundle_ref.v1",
        "cq.snb.bundle.v1",
        "cq.snb.node.v1",
        "cq.snb.edge.v1",
        "cq.snb.slice.v1",
        "cq.snb.degrade.v1",
        "cq.snb.bundle_meta.v1",
        "cq.snb.graph_summary.v1",
    }

    assert set(DETAILS_KIND_REGISTRY.keys()) == expected_kinds


@pytest.mark.parametrize(
    ("struct_type", "kind"),
    [
        (SemanticNeighborhoodBundleV1, "cq.snb.bundle.v1"),
        (SemanticNeighborhoodBundleRefV1, "cq.snb.bundle_ref.v1"),
        (SemanticNodeRefV1, "cq.snb.node.v1"),
        (SemanticEdgeV1, "cq.snb.edge.v1"),
        (NeighborhoodSliceV1, "cq.snb.slice.v1"),
        (DegradeEventV1, "cq.snb.degrade.v1"),
        (BundleMetaV1, "cq.snb.bundle_meta.v1"),
        (NeighborhoodGraphSummaryV1, "cq.snb.graph_summary.v1"),
    ],
)
def test_registry_kind_mapping(
    struct_type: type[msgspec.Struct],
    kind: str,
) -> None:
    """Test registry maps each kind to correct struct type."""
    assert DETAILS_KIND_REGISTRY[kind] is struct_type
