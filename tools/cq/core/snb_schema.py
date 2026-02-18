"""Semantic Neighborhood Bundle (SNB) schema definitions.

All SNB structures use frozen msgspec.Struct for deterministic serialization.
This is the CANONICAL schema authority for all downstream SNB operations.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal

from tools.cq.core.structs import CqStruct


class ArtifactPointerV1(CqStruct, frozen=True):
    """Generic artifact pointer with deterministic identity.

    Used for lazy loading and cache key computation.

    Parameters
    ----------
    artifact_kind : str
        Artifact category (e.g. "snb.bundle", "semantic.call_graph").
    artifact_id : str
        Unique identifier for this artifact.
    deterministic_id : str
        SHA256 of normalized content for cache keying.
    byte_size : int
        Artifact payload size in bytes.
    storage_path : str | None
        Optional filesystem or object-store path.
    metadata : dict[str, object] | None
        Optional key-value metadata.
    """

    artifact_kind: str
    artifact_id: str
    deterministic_id: str
    byte_size: int = 0
    storage_path: str | None = None
    metadata: dict[str, object] | None = None


class DegradeEventV1(CqStruct, frozen=True):
    """Typed degradation event for structured failure tracking.

    Captures partial enrichment failures with enough context for
    programmatic filtering and diagnostic analysis.

    Parameters
    ----------
    stage : str
        Enrichment stage identifier (e.g. "semantic.rust", "structural.interval_index").
    severity : Literal["info", "warning", "error"]
        Event severity level.
    category : str
        Error category (e.g. "timeout", "unavailable", "parse_error").
    message : str
        Human-readable diagnostic message.
    correlation_key : str | None
        Optional key for grouping related events.
    """

    stage: str
    severity: Literal["info", "warning", "error"] = "warning"
    category: str = ""
    message: str = ""
    correlation_key: str | None = None


class SemanticNodeRefV1(CqStruct, frozen=True):
    """Reference to a semantic node in the neighborhood.

    Minimal projection for neighborhood slice membership.
    Full node details live in node_index.

    Parameters
    ----------
    node_id : str
        Unique node identifier.
    kind : str
        Node kind (e.g. "function", "class", "import").
    name : str
        Node name or identifier.
    display_label : str
        Human-readable display label.
    file_path : str
        Source file path.
    byte_span : tuple[int, int] | None
        Byte offset range (bstart, bend).
    signature : str | None
        Optional function/method signature.
    qualname : str | None
        Optional qualified name.
    """

    node_id: str
    kind: str
    name: str
    display_label: str = ""
    file_path: str = ""
    byte_span: tuple[int, int] | None = None
    signature: str | None = None
    qualname: str | None = None


class SemanticEdgeV1(CqStruct, frozen=True):
    """Directed semantic edge in the neighborhood graph.

    Parameters
    ----------
    edge_id : str
        Unique edge identifier.
    source_node_id : str
        Source node ID.
    target_node_id : str
        Target node ID.
    edge_kind : str
        Edge relationship type (e.g. "calls", "imports", "extends").
    weight : float
        Edge weight for ranking or confidence.
    evidence_source : str
        Evidence source (e.g. "ast", "semantic.rust", "bytecode").
    metadata : dict[str, object] | None
        Optional edge metadata.
    """

    edge_id: str
    source_node_id: str
    target_node_id: str
    edge_kind: str
    weight: float = 1.0
    evidence_source: str = ""
    metadata: dict[str, object] | None = None


NeighborhoodSliceKind = Literal[
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
]


class NeighborhoodSliceV1(CqStruct, frozen=True):
    """A single slice of the semantic neighborhood.

    Each slice represents a specific relationship type (callers, imports, etc.)
    with progressive disclosure support (preview + total count).

    Parameters
    ----------
    kind : NeighborhoodSliceKind
        Slice relationship type.
    title : str
        Human-readable slice title.
    total : int
        Total count of nodes in this slice.
    preview : tuple[SemanticNodeRefV1, ...]
        Preview subset of nodes (progressive disclosure).
    edges : tuple[SemanticEdgeV1, ...]
        Edges connecting subject to slice members.
    collapsed : bool
        Whether slice is collapsed in UI.
    metadata : dict[str, object] | None
        Optional slice metadata.
    """

    kind: NeighborhoodSliceKind
    title: str
    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    edges: tuple[SemanticEdgeV1, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None


class SemanticNeighborhoodBundleRefV1(CqStruct, frozen=True):
    """Reference pointer to a neighborhood bundle artifact.

    Enables lazy loading and artifact caching.

    Parameters
    ----------
    bundle_id : str
        Unique bundle identifier.
    deterministic_id : str
        SHA256 of normalized bundle content.
    byte_size : int
        Bundle payload size in bytes.
    artifact_path : str | None
        Optional artifact storage path.
    preview_slices : tuple[NeighborhoodSliceKind, ...]
        Preview of available slice kinds.
    subject_node_id : str
        Subject node identifier.
    subject_label : str
        Subject node display label.
    """

    bundle_id: str
    deterministic_id: str
    byte_size: int = 0
    artifact_path: str | None = None
    preview_slices: tuple[NeighborhoodSliceKind, ...] = ()
    subject_node_id: str = ""
    subject_label: str = ""


class BundleMetaV1(CqStruct, frozen=True):
    """Bundle creation metadata and provenance.

    Parameters
    ----------
    tool : str
        Tool name (default: "cq").
    tool_version : str | None
        Optional tool version.
    workspace_root : str | None
        Workspace root path.
    query_text : str | None
        Original query text.
    created_at_ms : float | None
        Creation timestamp (milliseconds since epoch).
    semantic_sources : tuple[dict[str, object], ...]
        Static semantic source descriptors.
    limits : dict[str, int] | None
        Query limits applied.
    """

    tool: str = "cq"
    tool_version: str | None = None
    workspace_root: str | None = None
    query_text: str | None = None
    created_at_ms: float | None = None
    semantic_sources: tuple[dict[str, object], ...] = ()
    limits: dict[str, int] | None = None


class NeighborhoodGraphSummaryV1(CqStruct, frozen=True):
    """Lightweight graph summary statistics.

    Parameters
    ----------
    node_count : int
        Total node count.
    edge_count : int
        Total edge count.
    full_graph_artifact : ArtifactPointerV1 | None
        Optional pointer to full graph artifact.
    """

    node_count: int = 0
    edge_count: int = 0
    full_graph_artifact: ArtifactPointerV1 | None = None


class SemanticNeighborhoodBundleV1(CqStruct, frozen=True):
    """Complete semantic neighborhood bundle — CANONICAL definition.

    All downstream sections (R5, R6, R7) MUST reference only these fields.
    This is the authoritative schema that all SNB operations compile against.

    Combines structural and optional static enrichment planes into
    a single versioned, deterministic artifact.

    Parameters
    ----------
    bundle_id : str
        Unique bundle identifier.
    subject : SemanticNodeRefV1 | None
        Subject node reference.
    subject_label : str
        Subject display label.
    meta : BundleMetaV1 | None
        Bundle metadata.
    slices : tuple[NeighborhoodSliceV1, ...]
        Relationship slices.
    graph : NeighborhoodGraphSummaryV1 | None
        Graph summary statistics.
    node_index : Mapping[str, SemanticNodeRefV1] | None
        Full node index (node_id -> node).
    artifacts : tuple[ArtifactPointerV1, ...]
        Related artifacts.
    diagnostics : tuple[DegradeEventV1, ...]
        Degradation events.
    schema_version : str
        Schema version identifier.
    """

    bundle_id: str
    subject: SemanticNodeRefV1 | None = None
    subject_label: str = ""
    meta: BundleMetaV1 | None = None
    slices: tuple[NeighborhoodSliceV1, ...] = ()
    graph: NeighborhoodGraphSummaryV1 | None = None
    node_index: Mapping[str, SemanticNodeRefV1] | None = None
    artifacts: tuple[ArtifactPointerV1, ...] = ()
    diagnostics: tuple[DegradeEventV1, ...] = ()
    schema_version: str = "cq.snb.v1"


def freeze_node_index(
    node_index: Mapping[str, SemanticNodeRefV1] | None,
) -> Mapping[str, SemanticNodeRefV1] | None:
    """Return an immutable copy of node-index mapping payload."""
    if node_index is None:
        return None
    return MappingProxyType(dict(node_index))


__all__ = [
    "ArtifactPointerV1",
    "BundleMetaV1",
    "DegradeEventV1",
    "NeighborhoodGraphSummaryV1",
    "NeighborhoodSliceKind",
    "NeighborhoodSliceV1",
    "SemanticEdgeV1",
    "SemanticNeighborhoodBundleRefV1",
    "SemanticNeighborhoodBundleV1",
    "SemanticNodeRefV1",
    "freeze_node_index",
]
