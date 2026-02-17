"""Static semantic neighborhood bundle assembler."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from tools.cq.core.schema import ms
from tools.cq.core.snb_schema import (
    ArtifactPointerV1,
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)
from tools.cq.core.structs import CqStruct
from tools.cq.neighborhood.contracts import (
    TreeSitterNeighborhoodCollectRequest,
    plan_feasible_slices,
)
from tools.cq.neighborhood.tree_sitter_collector import (
    collect_tree_sitter_neighborhood,
)


class BundleBuildRequest(CqStruct, frozen=True):
    """Request to build a semantic neighborhood bundle."""

    target_name: str
    target_file: str
    root: Path
    language: str = "python"
    symbol_hint: str | None = None
    top_k: int = 10
    enable_semantic_enrichment: bool = True
    incremental_enrichment_enabled: bool = True
    incremental_enrichment_mode: str = "ts_sym"
    artifact_dir: Path | None = None
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    allow_symbol_fallback: bool = True
    target_degrade_events: tuple[DegradeEventV1, ...] = ()


def build_neighborhood_bundle(
    request: BundleBuildRequest,
) -> SemanticNeighborhoodBundleV1:
    """Build semantic neighborhood bundle with tree-sitter assembly.

    Returns:
        SemanticNeighborhoodBundleV1: Assembled semantic neighborhood bundle payload.
    """
    started = ms()
    collect_result = collect_tree_sitter_neighborhood(
        TreeSitterNeighborhoodCollectRequest(
            root=str(request.root),
            target_name=request.target_name,
            target_file=request.target_file,
            language=request.language,
            target_line=request.target_line,
            target_col=request.target_col,
            max_per_slice=request.top_k,
            slice_limits=_default_slice_limits(request.top_k),
        )
    )

    diagnostics = list(request.target_degrade_events)
    diagnostics.extend(collect_result.diagnostics)
    if request.enable_semantic_enrichment and collect_result.slices:
        diagnostics.append(
            DegradeEventV1(
                stage="semantic.enrichment",
                severity="info",
                category="tree_sitter_structural",
                message="Neighborhood enrichment assembled from tree-sitter structural planes",
            )
        )

    merged_slices = _merge_slices(tuple(collect_result.slices), request.top_k)
    subject = collect_result.subject or _build_fallback_subject_node(request)
    graph = _build_graph_summary(tuple(merged_slices))
    artifacts = _store_artifacts_with_preview(request.artifact_dir, merged_slices)

    return SemanticNeighborhoodBundleV1(
        bundle_id=_generate_bundle_id(request),
        subject=subject,
        subject_label=(subject.display_label if subject is not None else ""),
        meta=_build_meta(request, started),
        slices=tuple(merged_slices),
        graph=graph,
        node_index=None,
        artifacts=tuple(artifacts),
        diagnostics=tuple(diagnostics),
        schema_version="cq.snb.v1",
    )


def _default_slice_limits(top_k: int) -> dict[str, int]:
    limit = max(1, top_k)
    return {
        "parents": limit,
        "children": limit,
        "siblings": limit,
        "callers": limit,
        "callees": limit,
        "enclosing_context": max(1, min(3, limit)),
    }


def _merge_slices(
    slices: tuple[NeighborhoodSliceV1, ...],
    top_k: int,
) -> list[NeighborhoodSliceV1]:
    merged: dict[str, NeighborhoodSliceV1] = {}
    for slice_item in slices:
        existing = merged.get(slice_item.kind)
        if existing is None:
            merged[slice_item.kind] = slice_item
            continue

        preview = list(existing.preview)
        seen = {node.node_id for node in preview}
        for node in slice_item.preview:
            if node.node_id in seen:
                continue
            seen.add(node.node_id)
            preview.append(node)
            if len(preview) >= max(1, top_k):
                break

        edges = list(existing.edges)
        seen_edges = {edge.edge_id for edge in edges}
        for edge in slice_item.edges:
            if edge.edge_id in seen_edges:
                continue
            seen_edges.add(edge.edge_id)
            edges.append(edge)

        merged[slice_item.kind] = NeighborhoodSliceV1(
            kind=existing.kind,
            title=existing.title,
            total=existing.total + slice_item.total,
            preview=tuple(preview[: max(1, top_k)]),
            edges=tuple(edges),
            collapsed=existing.collapsed,
            metadata=existing.metadata,
        )

    return [merged[key] for key in sorted(merged)]


def _generate_bundle_id(request: BundleBuildRequest) -> str:
    raw = f"{request.language}:{request.target_file}:{request.target_name}:{request.target_line}:{request.target_col}"
    digest = hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"snb.{digest}"


def _build_fallback_subject_node(request: BundleBuildRequest) -> SemanticNodeRefV1:
    fallback_name = request.target_name or (request.symbol_hint or "target")
    return SemanticNodeRefV1(
        node_id=f"subject:{request.target_file}:{request.target_line or 0}:{fallback_name}",
        kind="unknown",
        name=fallback_name,
        display_label=fallback_name,
        file_path=request.target_file,
    )


def _build_graph_summary(
    slices: tuple[NeighborhoodSliceV1, ...],
) -> NeighborhoodGraphSummaryV1:
    node_ids: set[str] = set()
    edge_ids: set[str] = set()
    for slice_item in slices:
        for node in slice_item.preview:
            node_ids.add(node.node_id)
        for edge in slice_item.edges:
            edge_ids.add(edge.edge_id)
    return NeighborhoodGraphSummaryV1(
        node_count=len(node_ids),
        edge_count=len(edge_ids),
        full_graph_artifact=None,
    )


def _store_artifacts_with_preview(
    artifact_dir: Path | None,
    slices: list[NeighborhoodSliceV1],
) -> list[ArtifactPointerV1]:
    if artifact_dir is None:
        return []

    pointers: list[ArtifactPointerV1] = []
    for slice_item in slices:
        if slice_item.total <= len(slice_item.preview):
            continue

        payload = {
            "kind": slice_item.kind,
            "title": slice_item.title,
            "total": slice_item.total,
            "preview": [
                {
                    "node_id": node.node_id,
                    "kind": node.kind,
                    "name": node.name,
                    "display_label": node.display_label,
                    "file_path": node.file_path,
                }
                for node in slice_item.preview
            ],
        }
        content = json.dumps(payload, sort_keys=True, ensure_ascii=True)
        deterministic_id = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()

        artifact_path = artifact_dir / f"{slice_item.kind}-{deterministic_id[:12]}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(content, encoding="utf-8")

        pointers.append(
            ArtifactPointerV1(
                artifact_kind=f"snb.slice.{slice_item.kind}",
                artifact_id=f"{slice_item.kind}:{deterministic_id[:12]}",
                deterministic_id=deterministic_id,
                byte_size=len(content.encode("utf-8", errors="replace")),
                storage_path=str(artifact_path),
                metadata={"kind": slice_item.kind, "total": slice_item.total},
            )
        )

    return pointers


def _build_meta(
    request: BundleBuildRequest,
    started_ms: float,
) -> BundleMetaV1:
    return BundleMetaV1(
        tool="cq",
        tool_version=None,
        workspace_root=str(request.root),
        query_text=request.target_name,
        created_at_ms=max(0.0, ms() - started_ms),
        semantic_sources=(),
        limits={"top_k": max(1, request.top_k)},
    )


__all__ = [
    "BundleBuildRequest",
    "build_neighborhood_bundle",
    "plan_feasible_slices",
]
