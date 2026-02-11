# ruff: noqa: DOC201,PLR0913,PLR0914,C901
"""Semantic neighborhood bundle assembler with capability-gated enrichment.

This module orchestrates assembly of ``SemanticNeighborhoodBundleV1`` artifacts
with deterministic section ordering, capability-gated LSP planning, structured
degradation tracking, and preview/artifact split for heavy payloads.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, cast

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
from tools.cq.neighborhood.capability_gates import (
    normalize_capability_snapshot,
)
from tools.cq.neighborhood.capability_gates import (
    plan_feasible_slices as plan_capability_feasible_slices,
)
from tools.cq.neighborhood.pyrefly_adapter import collect_pyrefly_slices
from tools.cq.neighborhood.structural_collector import collect_structural_neighborhood
from tools.cq.search.rust_lsp_contracts import LspCapabilitySnapshotV1


class BundleBuildRequest(CqStruct, frozen=True):
    """Request to build a semantic neighborhood bundle.

    Takes `ScanSnapshot` (not ScanContext) per the repository architecture.
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
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    allow_symbol_fallback: bool = True
    target_degrade_events: tuple[DegradeEventV1, ...] = ()


def build_neighborhood_bundle(
    request: BundleBuildRequest,
) -> SemanticNeighborhoodBundleV1:
    """Build semantic neighborhood bundle with capability-gated assembly."""
    started = ms()
    degrade_events: list[DegradeEventV1] = list(request.target_degrade_events)

    structural_slices, structural_degrades = collect_structural_neighborhood(
        target_name=request.target_name,
        target_file=request.target_file,
        snapshot=request.snapshot,
        target_line=request.target_line,
        target_col=request.target_col,
        max_per_slice=request.top_k,
        slice_limits=_default_slice_limits(request.top_k),
    )
    degrade_events.extend(structural_degrades)

    lsp_slices: list[NeighborhoodSliceV1] = []
    lsp_env: dict[str, object] = {}
    if request.enable_lsp:
        negotiated_caps = _get_negotiated_caps(request)
        feasible_slices, cap_degrades = plan_feasible_slices(
            requested_slices=_lsp_slice_kinds(),
            capabilities=negotiated_caps,
        )
        degrade_events.extend(cap_degrades)

        lsp_slices_result, lsp_degrades, lsp_env = _collect_lsp_slices(request, feasible_slices)
        lsp_slices.extend(lsp_slices_result)
        degrade_events.extend(lsp_degrades)

    all_slices = _merge_slices(structural_slices, tuple(lsp_slices), request.top_k)
    artifacts = _store_artifacts_with_preview(request.artifact_dir, all_slices)
    subject_node = _build_subject_node(request)
    graph_summary = _build_graph_summary(structural_slices, tuple(lsp_slices))
    meta = _build_meta(request, started, lsp_env)

    return SemanticNeighborhoodBundleV1(
        bundle_id=_generate_bundle_id(request),
        subject=subject_node,
        subject_label=subject_node.display_label if subject_node else "",
        meta=meta,
        slices=tuple(all_slices),
        graph=graph_summary,
        node_index=None,
        artifacts=tuple(artifacts),
        diagnostics=tuple(degrade_events),
        schema_version="cq.snb.v1",
    )


def plan_feasible_slices(
    requested_slices: tuple[NeighborhoodSliceKind, ...],
    capabilities: Mapping[str, object] | LspCapabilitySnapshotV1 | None,
) -> tuple[tuple[NeighborhoodSliceKind, ...], tuple[DegradeEventV1, ...]]:
    """Plan which LSP slices are feasible given server capabilities."""
    return plan_capability_feasible_slices(
        requested_slices=requested_slices,
        capabilities=capabilities,
        stage="lsp.planning",
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


def _lsp_slice_kinds() -> tuple[NeighborhoodSliceKind, ...]:
    return (
        "references",
        "implementations",
        "type_supertypes",
        "type_subtypes",
    )


def _get_negotiated_caps(request: BundleBuildRequest) -> dict[str, object]:
    if not request.enable_lsp:
        return {}

    if request.language == "python":
        try:
            from tools.cq.search.pyrefly_lsp import get_pyrefly_lsp_capabilities

            return get_pyrefly_lsp_capabilities(request.root)
        except Exception:  # noqa: BLE001 - fail-open
            return {}

    if request.language == "rust":
        try:
            from tools.cq.search.rust_lsp import get_rust_lsp_capabilities

            return get_rust_lsp_capabilities(request.root)
        except Exception:  # noqa: BLE001 - fail-open
            return {}

    return {}


def _collect_lsp_slices(
    request: BundleBuildRequest,
    feasible_slices: tuple[NeighborhoodSliceKind, ...],
) -> tuple[list[NeighborhoodSliceV1], list[DegradeEventV1], dict[str, object]]:
    if not feasible_slices:
        return [], [], {}

    if request.language == "python":
        slices, degrades, env = collect_pyrefly_slices(
            root=request.root,
            target_file=request.target_file,
            target_line=request.target_line,
            target_col=request.target_col,
            target_name=request.target_name,
            feasible_slices=feasible_slices,
            top_k=request.top_k,
            symbol_hint=request.symbol_hint,
        )
        return list(slices), list(degrades), env

    if request.language == "rust":
        return _collect_rust_slices(request, feasible_slices)

    return (
        [],
        [
            DegradeEventV1(
                stage="lsp.collection",
                severity="info",
                category="unsupported_language",
                message=f"No LSP adapter configured for language '{request.language}'",
            )
        ],
        {},
    )


def _collect_rust_slices(
    request: BundleBuildRequest,
    feasible_slices: tuple[NeighborhoodSliceKind, ...],
) -> tuple[list[NeighborhoodSliceV1], list[DegradeEventV1], dict[str, object]]:
    from tools.cq.search.rust_lsp import RustLspRequest, enrich_with_rust_lsp

    if not request.target_file or request.target_line is None:
        return (
            [],
            [
                DegradeEventV1(
                    stage="lsp.rust",
                    severity="warning",
                    category="missing_anchor",
                    message="Rust LSP neighborhood slices require resolved file:line target",
                )
            ],
            {},
        )

    payload = enrich_with_rust_lsp(
        RustLspRequest(
            file_path=str((request.root / request.target_file).resolve()),
            line=request.target_line,
            col=max(0, request.target_col or 0),
            query_intent="neighborhood",
        ),
        root=request.root,
    )
    if not isinstance(payload, Mapping):
        return (
            [],
            [
                DegradeEventV1(
                    stage="lsp.rust",
                    severity="warning",
                    category="unavailable",
                    message="Rust LSP enrichment unavailable for resolved target",
                )
            ],
            {},
        )

    subject_id = f"target.{request.target_file}:{request.target_name}"
    slices: list[NeighborhoodSliceV1] = []

    symbol_grounding: Mapping[str, object] = {}
    symbol_grounding_raw = payload.get("symbol_grounding")
    if isinstance(symbol_grounding_raw, Mapping):
        symbol_grounding = symbol_grounding_raw
    type_hierarchy: Mapping[str, object] = {}
    type_hierarchy_raw = payload.get("type_hierarchy")
    if isinstance(type_hierarchy_raw, Mapping):
        type_hierarchy = type_hierarchy_raw

    for kind in feasible_slices:
        if kind == "references":
            slices.append(
                _lsp_slice_from_rows(
                    kind="references",
                    title="References",
                    rows=_mapping_rows(symbol_grounding.get("references")),
                    subject_id=subject_id,
                    edge_kind="references",
                    top_k=request.top_k,
                    source="lsp.rust",
                )
            )
            continue
        if kind == "implementations":
            slices.append(
                _lsp_slice_from_rows(
                    kind="implementations",
                    title="Implementations",
                    rows=_mapping_rows(symbol_grounding.get("implementations")),
                    subject_id=subject_id,
                    edge_kind="implements",
                    top_k=request.top_k,
                    source="lsp.rust",
                )
            )
            continue
        if kind == "type_supertypes":
            slices.append(
                _lsp_slice_from_rows(
                    kind="type_supertypes",
                    title="Supertypes",
                    rows=_mapping_rows(type_hierarchy.get("supertypes")),
                    subject_id=subject_id,
                    edge_kind="extends",
                    top_k=request.top_k,
                    source="lsp.rust",
                )
            )
            continue
        if kind == "type_subtypes":
            slices.append(
                _lsp_slice_from_rows(
                    kind="type_subtypes",
                    title="Subtypes",
                    rows=_mapping_rows(type_hierarchy.get("subtypes")),
                    subject_id=subject_id,
                    edge_kind="subtype",
                    top_k=request.top_k,
                    source="lsp.rust",
                )
            )

    lsp_env = _extract_lsp_env(payload)
    if not slices:
        return (
            [],
            [
                DegradeEventV1(
                    stage="lsp.rust",
                    severity="info",
                    category="no_signal",
                    message="Rust LSP returned no neighborhood slice signals for this target",
                )
            ],
            lsp_env,
        )
    return slices, [], lsp_env


def _mapping_rows(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [cast("Mapping[str, object]", row) for row in value if isinstance(row, Mapping)]


def _lsp_slice_from_rows(
    *,
    kind: NeighborhoodSliceKind,
    title: str,
    rows: list[Mapping[str, object]],
    subject_id: str,
    edge_kind: str,
    top_k: int,
    source: str,
) -> NeighborhoodSliceV1:
    preview_rows = rows[: max(0, top_k)]

    preview: list[SemanticNodeRefV1] = []
    edges: list[SemanticEdgeV1] = []
    for row in preview_rows:
        node = _lsp_row_to_node(row, source=source)
        preview.append(node)
        edges.append(
            SemanticEdgeV1(
                edge_id=f"{subject_id}→{node.node_id}:{edge_kind}",
                source_node_id=subject_id,
                target_node_id=node.node_id,
                edge_kind=edge_kind,
                evidence_source=source,
            )
        )

    return NeighborhoodSliceV1(
        kind=kind,
        title=title,
        total=len(rows),
        preview=tuple(preview),
        edges=tuple(edges),
        collapsed=True,
    )


def _lsp_row_to_node(row: Mapping[str, object], *, source: str) -> SemanticNodeRefV1:
    uri = row.get("uri")
    uri_value = uri if isinstance(uri, str) else ""

    file_path = ""
    if uri_value.startswith("file://"):
        file_path = uri_value.removeprefix("file://")

    line = row.get("range_start_line")
    col = row.get("range_start_col")
    line_value = line if isinstance(line, int) else 0
    col_value = col if isinstance(col, int) else 0

    name = row.get("name")
    if not isinstance(name, str) or not name:
        name = Path(file_path).name if file_path else uri_value
    if not name:
        name = "<unknown>"

    return SemanticNodeRefV1(
        node_id=f"{source}.symbol.{file_path}:{line_value}:{col_value}:{name}",
        kind="symbol",
        name=name,
        display_label=name,
        file_path=file_path,
    )


def _extract_lsp_env(payload: Mapping[str, object]) -> dict[str, object]:
    session_env = payload.get("session_env")
    if not isinstance(session_env, Mapping):
        return {}
    env: dict[str, object] = {}
    for key_in, key_out in (
        ("workspace_health", "lsp_health"),
        ("quiescent", "lsp_quiescent"),
        ("position_encoding", "lsp_position_encoding"),
    ):
        value = session_env.get(key_in)
        if value is not None:
            env[key_out] = value
    return env


def _merge_slices(
    structural: tuple[NeighborhoodSliceV1, ...],
    lsp: tuple[NeighborhoodSliceV1, ...],
    top_k: int,
) -> list[NeighborhoodSliceV1]:
    all_slices: list[NeighborhoodSliceV1] = []

    kind_map: dict[str, NeighborhoodSliceV1] = {}
    for slice_ in structural:
        kind_map[slice_.kind] = slice_
    for slice_ in lsp:
        kind_map[slice_.kind] = slice_

    for slice_ in kind_map.values():
        if len(slice_.preview) > top_k:
            all_slices.append(
                NeighborhoodSliceV1(
                    kind=slice_.kind,
                    title=slice_.title,
                    total=slice_.total,
                    preview=slice_.preview[:top_k],
                    edges=slice_.edges[:top_k] if len(slice_.edges) > top_k else slice_.edges,
                    collapsed=slice_.collapsed,
                    metadata=slice_.metadata,
                )
            )
            continue
        all_slices.append(slice_)

    return all_slices


def _store_artifacts_with_preview(
    artifact_dir: Path | None,
    slices: list[NeighborhoodSliceV1],
) -> list[ArtifactPointerV1]:
    if artifact_dir is None:
        return []

    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[ArtifactPointerV1] = []

    for slice_ in slices:
        if slice_.total <= len(slice_.preview):
            continue

        artifact_id = f"slice_{slice_.kind}"
        artifact_path = artifact_dir / f"{artifact_id}.json"
        full_data = {
            "kind": slice_.kind,
            "title": slice_.title,
            "total": slice_.total,
            "preview": [_node_to_dict(node) for node in slice_.preview],
            "edges": [_edge_to_dict(edge) for edge in slice_.edges],
        }
        artifact_path.write_text(json.dumps(full_data, indent=2))
        payload_bytes = artifact_path.read_bytes()
        artifacts.append(
            ArtifactPointerV1(
                artifact_kind="snb.slice",
                artifact_id=artifact_id,
                deterministic_id=hashlib.sha256(payload_bytes).hexdigest(),
                byte_size=len(payload_bytes),
                storage_path=str(artifact_path),
                metadata={"slice_kind": slice_.kind, "format": "json"},
            )
        )

    return artifacts


def _node_to_dict(node: SemanticNodeRefV1) -> dict[str, object]:
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
    # Prefer anchor-based resolution when available.
    if request.target_file and request.target_line is not None:
        candidates = [
            record
            for record in request.snapshot.def_records
            if record.file == request.target_file
            and record.start_line <= request.target_line <= record.end_line
        ]
        if candidates:
            candidates.sort(
                key=lambda record: (
                    max(0, record.end_line - record.start_line),
                    record.start_line,
                    record.start_col,
                    record.text,
                )
            )
            subject = candidates[0]
            name = _extract_name_from_text(subject.text)
            return SemanticNodeRefV1(
                node_id=f"structural.{subject.kind}.{subject.file}:{subject.start_line}:{subject.start_col}",
                kind=subject.kind,
                name=name,
                display_label=name,
                file_path=subject.file,
                byte_span=None,
            )

    # Fallback name+file resolution.
    for def_rec in request.snapshot.def_records:
        if def_rec.file != request.target_file:
            continue
        if _extract_name_from_text(def_rec.text) != request.target_name:
            continue
        return SemanticNodeRefV1(
            node_id=f"structural.{def_rec.kind}.{def_rec.file}:{def_rec.start_line}:{def_rec.start_col}",
            kind=def_rec.kind,
            name=request.target_name,
            display_label=request.target_name,
            file_path=def_rec.file,
            byte_span=None,
        )

    return SemanticNodeRefV1(
        node_id=f"target.{request.target_file}:{request.target_name}",
        kind="unknown",
        name=request.target_name,
        display_label=request.target_name,
        file_path=request.target_file,
    )


def _extract_name_from_text(text: str) -> str:
    raw = text.strip()
    if raw.startswith(("def ", "class ")):
        head = raw.split("(", 1)[0]
        if head:
            return head.split()[-1]
    if "(" in raw:
        call_head = raw.split("(", 1)[0]
        if "." in call_head:
            return call_head.split(".")[-1]
        return call_head.strip()
    return raw


def _build_graph_summary(
    structural: tuple[NeighborhoodSliceV1, ...],
    lsp: tuple[NeighborhoodSliceV1, ...],
) -> NeighborhoodGraphSummaryV1:
    total_nodes = 0
    total_edges = 0
    for slice_ in structural:
        total_nodes += slice_.total
        total_edges += len(slice_.edges)
    for slice_ in lsp:
        total_nodes += slice_.total
        total_edges += len(slice_.edges)
    return NeighborhoodGraphSummaryV1(node_count=total_nodes, edge_count=total_edges)


def _build_meta(
    request: BundleBuildRequest,
    started: float,
    lsp_env: dict[str, object],
) -> BundleMetaV1:
    lsp_servers: tuple[dict[str, object], ...] = ()
    if request.enable_lsp:
        normalized_caps = normalize_capability_snapshot(_get_negotiated_caps(request))
        lsp_servers = (
            {
                "language": request.language,
                "position_encoding": lsp_env.get("lsp_position_encoding", "utf-16"),
                "workspace_health": lsp_env.get("lsp_health", "unknown"),
                "quiescent": bool(lsp_env.get("lsp_quiescent")),
                "capabilities": {
                    "server_caps": normalized_caps.server_caps,
                    "client_caps": normalized_caps.client_caps,
                    "experimental_caps": normalized_caps.experimental_caps,
                },
            },
        )

    return BundleMetaV1(
        tool="cq",
        tool_version=None,
        workspace_root=str(request.root),
        query_text=request.symbol_hint,
        created_at_ms=ms() - started,
        lsp_servers=lsp_servers,
        limits={"top_k": request.top_k},
    )


def _generate_bundle_id(request: BundleBuildRequest) -> str:
    payload = (
        f"{request.target_file}:{request.target_name}:{request.language}:"
        f"{request.target_line}:{request.target_col}"
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


__all__ = [
    "BundleBuildRequest",
    "build_neighborhood_bundle",
    "plan_feasible_slices",
]
