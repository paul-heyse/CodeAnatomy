"""Semantic neighborhood bundle assembler with capability-gated enrichment.

This module orchestrates the assembly of SemanticNeighborhoodBundleV1 artifacts
with deterministic section ordering, capability-gated LSP planning, structured
degradation tracking, and preview/artifact split for heavy payloads.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.neighborhood.scan_snapshot import ScanSnapshot


from tools.cq.core.schema import ms
from tools.cq.core.snb_schema import (
    ArtifactPointerV1,
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceKind,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)
from tools.cq.core.structs import CqStruct
from tools.cq.neighborhood.structural_collector import collect_structural_neighborhood


class BundleBuildRequest(CqStruct, frozen=True):
    """Request to build a semantic neighborhood bundle.

    Takes ScanSnapshot (not ScanContext) per Optimization #2.

    Parameters
    ----------
    target_name : str
        Target symbol name.
    target_file : str
        Target file path.
    root : Path
        Repository root path.
    snapshot : ScanSnapshot
        Scan snapshot with indexed definitions.
    language : str
        Query language (e.g., "python", "rust").
    symbol_hint : str | None
        Optional symbol hint for disambiguation.
    top_k : int
        Maximum preview items per slice.
    enable_lsp : bool
        Whether to attempt LSP enrichment.
    artifact_dir : Path | None
        Optional directory for artifact storage.
    """

    target_name: str
    target_file: str
    root: Path
    snapshot: ScanSnapshot
    language: str = "python"
    symbol_hint: str | None = None
    top_k: int = 10
    enable_lsp: bool = True
    artifact_dir: Path | None = None


def build_neighborhood_bundle(
    request: BundleBuildRequest,
) -> SemanticNeighborhoodBundleV1:
    """Build semantic neighborhood bundle with capability-gated assembly.

    Assembly phases:
    1. Structural neighborhood (always available)
    2. Capability-gated LSP enrichment (if enabled)
    3. Deterministic section layout via slot map + SECTION_ORDER
    4. Artifact storage with preview/body split

    Parameters
    ----------
    request : BundleBuildRequest
        Bundle build request.

    Returns:
    -------
    SemanticNeighborhoodBundleV1
        Assembled bundle with deterministic ordering.
    """
    started = ms()
    degrade_events: list[DegradeEventV1] = []

    # Phase 1: structural neighborhood (always available)
    structural_slices, structural_degrades = collect_structural_neighborhood(
        target_name=request.target_name,
        target_file=request.target_file,
        snapshot=request.snapshot,
        max_per_slice=request.top_k,
    )
    degrade_events.extend(structural_degrades)

    # Phase 2: capability-gated LSP enrichment
    lsp_slices: list[NeighborhoodSliceV1] = []
    if request.enable_lsp:
        feasible_slices, cap_degrades = plan_feasible_slices(
            requested_slices=_lsp_slice_kinds(),
            capabilities=_get_negotiated_caps(request),
        )
        degrade_events.extend(cap_degrades)

        lsp_slices_result, lsp_degrades = _collect_lsp_slices(request, feasible_slices)
        lsp_slices.extend(lsp_slices_result)
        degrade_events.extend(lsp_degrades)

    # Phase 3: deterministic assembly
    all_slices = _merge_slices(structural_slices, tuple(lsp_slices), request.top_k)

    # Phase 4: artifacts with preview/body split
    artifacts = _store_artifacts_with_preview(request.artifact_dir, all_slices)

    # Build subject node
    subject_node = _build_subject_node(request)

    # Build graph summary
    graph_summary = _build_graph_summary(structural_slices, tuple(lsp_slices))

    # Build metadata
    meta = _build_meta(request, started)

    return SemanticNeighborhoodBundleV1(
        bundle_id=_generate_bundle_id(request),
        subject=subject_node,
        subject_label=subject_node.display_label if subject_node else "",
        meta=meta,
        slices=tuple(all_slices),
        graph=graph_summary,
        node_index=None,  # Optional: precomputed node lookup
        artifacts=tuple(artifacts),
        diagnostics=tuple(degrade_events),
        schema_version="cq.snb.v1",
    )


def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: dict[str, object],
) -> tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]:
    """Plan which LSP slices are feasible given server capabilities.

    This is a stub implementation. Full capability checking would inspect
    the capabilities dict and return only slices that can be served.

    Parameters
    ----------
    requested_slices : tuple[NeighborhoodSliceKind, ...]
        Requested slice kinds.
    capabilities : dict[str, object]
        LSP server capabilities.

    Returns:
    -------
    tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]
        (feasible_slices, degrade_events).
    """
    # Stub: LSP integration not yet wired; capabilities will gate slices later
    _ = capabilities
    degrades = [
        DegradeEventV1(
            stage="lsp.planning",
            severity="info",
            category="not_implemented",
            message=f"LSP slice '{kind}' planning not yet implemented",
        )
        for kind in requested_slices
    ]

    return (), tuple(degrades)


def _lsp_slice_kinds() -> tuple[NeighborhoodSliceKind, ...]:
    """Return LSP-enriched slice kinds.

    Returns:
    -------
    tuple[NeighborhoodSliceKind, ...]
        LSP slice kinds.
    """
    return (
        "references",
        "implementations",
        "type_supertypes",
        "type_subtypes",
    )


def _get_negotiated_caps(
    _request: BundleBuildRequest,
) -> dict[str, object]:
    """Get negotiated LSP capabilities.

    Stub: returns empty dict until LSP integration is wired.

    Parameters
    ----------
    request : BundleBuildRequest
        Build request.

    Returns:
    -------
    dict[str, object]
        LSP server capabilities.
    """
    return {}


def _collect_lsp_slices(
    _request: BundleBuildRequest,
    feasible_slices: tuple[NeighborhoodSliceKind, ...],
) -> tuple[list[NeighborhoodSliceV1], list[DegradeEventV1]]:
    """Collect LSP-enriched slices.

    Stub: returns empty lists until LSP integration is wired.

    Parameters
    ----------
    request : BundleBuildRequest
        Build request.
    feasible_slices : tuple[NeighborhoodSliceKind, ...]
        Feasible slice kinds from capability planning.

    Returns:
    -------
    tuple[list[NeighborhoodSliceV1], list[DegradeEventV1]]
        (lsp_slices, degrade_events).
    """
    degrades = [
        DegradeEventV1(
            stage="lsp.collection",
            severity="info",
            category="not_implemented",
            message=f"LSP collection for '{kind}' not yet implemented",
        )
        for kind in feasible_slices
    ]

    return [], degrades


def _merge_slices(
    structural: tuple[NeighborhoodSliceV1, ...],
    lsp: tuple[NeighborhoodSliceV1, ...],
    top_k: int,
) -> list[NeighborhoodSliceV1]:
    """Merge structural and LSP slices with preview truncation.

    Parameters
    ----------
    structural : tuple[NeighborhoodSliceV1, ...]
        Structural slices.
    lsp : tuple[NeighborhoodSliceV1, ...]
        LSP slices.
    top_k : int
        Maximum preview items per slice.

    Returns:
    -------
    list[NeighborhoodSliceV1]
        Merged slices with preview limits applied.
    """
    all_slices: list[NeighborhoodSliceV1] = []

    # Merge by kind: prefer LSP over structural for overlapping kinds
    kind_map: dict[str, NeighborhoodSliceV1] = {}

    for s in structural:
        kind_map[s.kind] = s

    for s in lsp:
        # LSP slices override structural slices
        kind_map[s.kind] = s

    # Apply top_k preview limit
    for slice_ in kind_map.values():
        if len(slice_.preview) > top_k:
            # Truncate preview to top_k
            truncated = NeighborhoodSliceV1(
                kind=slice_.kind,
                title=slice_.title,
                total=slice_.total,
                preview=slice_.preview[:top_k],
                edges=slice_.edges[:top_k] if len(slice_.edges) > top_k else slice_.edges,
                collapsed=slice_.collapsed,
                metadata=slice_.metadata,
            )
            all_slices.append(truncated)
        else:
            all_slices.append(slice_)

    return all_slices


def _store_artifacts_with_preview(
    artifact_dir: Path | None,
    slices: list[NeighborhoodSliceV1],
) -> list[ArtifactPointerV1]:
    """Store heavy payloads as artifacts. Preview stays in slice.

    Every heavy LSP surface uses:
    - small preview in section details (top-K items)
    - full payload in artifact file
    - deterministic artifact IDs and sizes in provenance

    Parameters
    ----------
    artifact_dir : Path | None
        Optional artifact directory.
    slices : list[NeighborhoodSliceV1]
        Slices to check for artifact storage.

    Returns:
    -------
    list[ArtifactPointerV1]
        Artifact pointers for stored payloads.
    """
    if artifact_dir is None:
        return []

    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[ArtifactPointerV1] = []

    for s in slices:
        # Only store artifacts when total > preview count (overflow)
        if s.total <= len(s.preview):
            continue

        artifact_id = f"slice_{s.kind}"
        artifact_path = artifact_dir / f"{artifact_id}.json"

        # Write full data (nodes + edges)
        # Per R1 schema: use s.preview field for nodes
        full_data = {
            "kind": s.kind,
            "title": s.title,
            "total": s.total,
            "preview": [_node_to_dict(n) for n in s.preview],
            "edges": [_edge_to_dict(e) for e in s.edges],
        }
        artifact_path.write_text(json.dumps(full_data, indent=2))

        # Record artifact metadata
        payload_bytes = artifact_path.read_bytes()
        artifacts.append(
            ArtifactPointerV1(
                artifact_kind="snb.slice",
                artifact_id=artifact_id,
                deterministic_id=hashlib.sha256(payload_bytes).hexdigest(),
                byte_size=len(payload_bytes),
                storage_path=str(artifact_path),
                metadata={"slice_kind": s.kind, "format": "json"},
            )
        )

    return artifacts


def _node_to_dict(node: SemanticNodeRefV1) -> dict[str, object]:
    """Convert SemanticNodeRefV1 to dict for JSON serialization.

    Parameters
    ----------
    node : SemanticNodeRefV1
        Node to convert.

    Returns:
    -------
    dict[str, object]
        Node dict.
    """
    return {
        "node_id": node.node_id,
        "kind": node.kind,
        "name": node.name,
        "display_label": node.display_label,
        "file_path": node.file_path,
        "byte_span": node.byte_span,
        "signature": node.signature,
        "qualname": node.qualname,
    }


def _edge_to_dict(edge: SemanticEdgeV1) -> dict[str, object]:
    """Convert SemanticEdgeV1 to dict for JSON serialization.

    Parameters
    ----------
    edge : SemanticEdgeV1
        Edge to convert.

    Returns:
    -------
    dict[str, object]
        Edge dict.
    """
    return {
        "edge_id": edge.edge_id,
        "source_node_id": edge.source_node_id,
        "target_node_id": edge.target_node_id,
        "edge_kind": edge.edge_kind,
        "weight": edge.weight,
        "evidence_source": edge.evidence_source,
        "metadata": edge.metadata,
    }


def _build_subject_node(request: BundleBuildRequest) -> SemanticNodeRefV1 | None:
    """Build subject node reference from request.

    Parameters
    ----------
    request : BundleBuildRequest
        Build request.

    Returns:
    -------
    SemanticNodeRefV1 | None
        Subject node or None if not found.
    """
    # Try to find the target definition in snapshot
    for def_rec in request.snapshot.def_records:
        if def_rec.file == request.target_file:
            # Simple name extraction heuristic
            name = _extract_name_from_text(def_rec.text)
            if name == request.target_name:
                return SemanticNodeRefV1(
                    node_id=f"structural.{def_rec.kind}.{def_rec.file}:{def_rec.start_line}:{def_rec.start_col}",
                    kind=def_rec.kind,
                    name=name,
                    display_label=name,
                    file_path=def_rec.file,
                    byte_span=None,
                )

    # Fallback: build minimal subject node
    return SemanticNodeRefV1(
        node_id=f"target.{request.target_file}:{request.target_name}",
        kind="unknown",
        name=request.target_name,
        display_label=request.target_name,
        file_path=request.target_file,
    )


def _extract_name_from_text(text: str) -> str:
    """Extract symbol name from record text.

    Parameters
    ----------
    text : str
        Record text.

    Returns:
    -------
    str
        Extracted name.
    """
    text = text.strip()

    # Function/class definitions: "def foo(...)" → "foo"
    if text.startswith(("def ", "class ")):
        parts = text.split("(", 1)
        if parts:
            return parts[0].split()[-1]

    # Method calls: "obj.method(...)" → "method"
    if "(" in text:
        call_part = text.split("(", 1)[0]
        if "." in call_part:
            return call_part.split(".")[-1]
        return call_part.strip()

    return text


def _build_graph_summary(
    structural: tuple[NeighborhoodSliceV1, ...],
    lsp: tuple[NeighborhoodSliceV1, ...],
) -> NeighborhoodGraphSummaryV1:
    """Build graph summary from slices.

    Parameters
    ----------
    structural : tuple[NeighborhoodSliceV1, ...]
        Structural slices.
    lsp : tuple[NeighborhoodSliceV1, ...]
        LSP slices.

    Returns:
    -------
    NeighborhoodGraphSummaryV1
        Graph summary statistics.
    """
    total_nodes = 0
    total_edges = 0

    for s in structural:
        total_nodes += s.total
        total_edges += len(s.edges)

    for s in lsp:
        total_nodes += s.total
        total_edges += len(s.edges)

    return NeighborhoodGraphSummaryV1(
        node_count=total_nodes,
        edge_count=total_edges,
    )


def _build_meta(request: BundleBuildRequest, started: float) -> BundleMetaV1:
    """Build bundle metadata.

    Parameters
    ----------
    request : BundleBuildRequest
        Build request.
    started : float
        Start timestamp (milliseconds).

    Returns:
    -------
    BundleMetaV1
        Bundle metadata.
    """
    return BundleMetaV1(
        tool="cq",
        tool_version=None,  # Could be injected from package metadata
        workspace_root=str(request.root),
        query_text=request.symbol_hint,
        created_at_ms=ms() - started,
        lsp_servers=(),  # No LSP servers yet
        limits={"top_k": request.top_k},
    )


def _generate_bundle_id(request: BundleBuildRequest) -> str:
    """Generate deterministic bundle ID.

    Parameters
    ----------
    request : BundleBuildRequest
        Build request.

    Returns:
    -------
    str
        Deterministic bundle ID.
    """
    # Simple deterministic ID based on target
    payload = f"{request.target_file}:{request.target_name}:{request.language}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


__all__ = [
    "BundleBuildRequest",
    "build_neighborhood_bundle",
    "plan_feasible_slices",
]
