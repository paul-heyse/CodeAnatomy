"""Structural neighborhood collector.

Collects structural relationships (parents, children, siblings, callers,
callees) from the ast-grep scan snapshot.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.utils.interval_index import IntervalIndex

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.snb_schema import (
    DegradeEventV1,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNodeRefV1,
)
from tools.cq.core.structs import CqStruct
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot


class StructuralNeighborhood(CqStruct, frozen=True):
    """Structural relationships from ast-grep scan snapshot."""

    parents: tuple[SgRecord, ...] = ()
    children: tuple[SgRecord, ...] = ()
    siblings: tuple[SgRecord, ...] = ()
    enclosing_context: SgRecord | None = None
    callers: tuple[SgRecord, ...] = ()
    callees: tuple[SgRecord, ...] = ()


class StructuralNeighborhoodCollectRequest(CqStruct, frozen=True):
    """Typed request for structural neighborhood collection."""

    target_name: str
    target_file: str
    snapshot: ScanSnapshot
    target_line: int | None = None
    target_col: int | None = None
    max_per_slice: int = 50
    slice_limits: Mapping[str, int] | None = None


class RecordSliceBuildRequest(CqStruct, frozen=True):
    """Request envelope for constructing one record-backed structural slice."""

    records: tuple[SgRecord, ...]
    slice_kind: str
    title: str
    edge_kind: str
    subject_node_id: str
    edge_direction: str
    max_per_slice: int
    slice_limits: Mapping[str, int] | None = None


def collect_structural_neighborhood(
    request: StructuralNeighborhoodCollectRequest,
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...]]:
    """Collect structural neighborhood slices from scan snapshot.

    Returns:
        Structural neighborhood slices and degrade events.
    """
    target_def, target_degrades = _find_target_definition(
        name=request.target_name,
        file=request.target_file,
        snapshot=request.snapshot,
        target_line=request.target_line,
        target_col=request.target_col,
    )
    if target_def is None:
        degrade = DegradeEventV1(
            stage="structural.target_resolution",
            severity="error",
            category="not_found",
            message=f"Target definition '{request.target_name}' not found in {request.target_file}",
        )
        return (), (*target_degrades, degrade)

    neighborhood = _collect_neighborhood(target_def, request.snapshot)
    subject_node_id = _node_id_from_record(target_def)
    slices = _build_structural_slices(
        neighborhood=neighborhood,
        subject_node_id=subject_node_id,
        max_per_slice=request.max_per_slice,
        slice_limits=request.slice_limits,
    )
    return tuple(slices), target_degrades


def _build_structural_slices(
    *,
    neighborhood: StructuralNeighborhood,
    subject_node_id: str,
    max_per_slice: int,
    slice_limits: Mapping[str, int] | None,
) -> list[NeighborhoodSliceV1]:
    slices: list[NeighborhoodSliceV1] = []
    _append_record_slice(
        slices,
        RecordSliceBuildRequest(
            records=neighborhood.parents,
            slice_kind="parents",
            title="Parent Scopes",
            edge_kind="contains",
            subject_node_id=subject_node_id,
            edge_direction="inbound",
            max_per_slice=max_per_slice,
            slice_limits=slice_limits,
        ),
    )
    _append_record_slice(
        slices,
        RecordSliceBuildRequest(
            records=neighborhood.children,
            slice_kind="children",
            title="Nested Definitions",
            edge_kind="contains",
            subject_node_id=subject_node_id,
            edge_direction="outbound",
            max_per_slice=max_per_slice,
            slice_limits=slice_limits,
        ),
    )
    _append_record_slice(
        slices,
        RecordSliceBuildRequest(
            records=neighborhood.siblings,
            slice_kind="siblings",
            title="Sibling Definitions",
            edge_kind="",
            subject_node_id=subject_node_id,
            edge_direction="none",
            max_per_slice=max_per_slice,
            slice_limits=slice_limits,
        ),
    )
    _append_enclosing_context_slice(slices, neighborhood.enclosing_context, subject_node_id)
    _append_record_slice(
        slices,
        RecordSliceBuildRequest(
            records=neighborhood.callees,
            slice_kind="callees",
            title="Functions Called",
            edge_kind="calls",
            subject_node_id=subject_node_id,
            edge_direction="outbound",
            max_per_slice=max_per_slice,
            slice_limits=slice_limits,
        ),
    )
    _append_record_slice(
        slices,
        RecordSliceBuildRequest(
            records=neighborhood.callers,
            slice_kind="callers",
            title="Callers",
            edge_kind="calls",
            subject_node_id=subject_node_id,
            edge_direction="inbound",
            max_per_slice=max_per_slice,
            slice_limits=slice_limits,
        ),
    )
    return slices


def _append_enclosing_context_slice(
    slices: list[NeighborhoodSliceV1],
    enclosing_context: SgRecord | None,
    subject_node_id: str,
) -> None:
    if enclosing_context is None:
        return
    enclosing_node = _record_to_node_ref(enclosing_context)
    enclosing_edge = SemanticEdgeV1(
        edge_id=f"{_node_id_from_record(enclosing_context)}→{subject_node_id}",
        source_node_id=_node_id_from_record(enclosing_context),
        target_node_id=subject_node_id,
        edge_kind="contains",
        evidence_source="structural.ast",
    )
    slices.append(
        NeighborhoodSliceV1(
            kind="enclosing_context",
            title="Enclosing Context",
            total=1,
            preview=(enclosing_node,),
            edges=(enclosing_edge,),
            collapsed=False,
        )
    )


def _append_record_slice(
    slices: list[NeighborhoodSliceV1],
    request: RecordSliceBuildRequest,
) -> None:
    if not request.records:
        return
    limited = _apply_limit(
        request.records,
        request.slice_kind,
        request.max_per_slice,
        request.slice_limits,
    )
    nodes = [_record_to_node_ref(record) for record in limited]
    edges = _build_slice_edges(
        records=limited,
        subject_node_id=request.subject_node_id,
        edge_kind=request.edge_kind,
        edge_direction=request.edge_direction,
    )
    slices.append(
        NeighborhoodSliceV1(
            kind=request.slice_kind,
            title=request.title,
            total=len(request.records),
            preview=tuple(nodes),
            edges=tuple(edges),
            collapsed=True,
        )
    )


def _build_slice_edges(
    *,
    records: tuple[SgRecord, ...],
    subject_node_id: str,
    edge_kind: str,
    edge_direction: str,
) -> list[SemanticEdgeV1]:
    if edge_direction == "none":
        return []
    edges: list[SemanticEdgeV1] = []
    for record in records:
        record_node_id = _node_id_from_record(record)
        source_node_id = record_node_id if edge_direction == "inbound" else subject_node_id
        target_node_id = subject_node_id if edge_direction == "inbound" else record_node_id
        edges.append(
            SemanticEdgeV1(
                edge_id=f"{source_node_id}→{target_node_id}",
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                edge_kind=edge_kind,
                evidence_source="structural.ast",
            )
        )
    return edges


def _find_target_definition(
    name: str,
    file: str,
    snapshot: ScanSnapshot,
    *,
    target_line: int | None,
    target_col: int | None,
) -> tuple[SgRecord | None, tuple[DegradeEventV1, ...]]:
    degrades: list[DegradeEventV1] = []
    target_file = _normalize_file_path(file)

    if target_line is not None and file:
        candidates = [
            record
            for record in snapshot.def_records
            if _normalize_file_path(record.file) == target_file
            and record.start_line <= target_line <= record.end_line
        ]
        if candidates:
            candidates.sort(key=_anchor_sort_key)
            if len(candidates) > 1:
                degrades.append(
                    DegradeEventV1(
                        stage="structural.target_resolution",
                        severity="warning",
                        category="ambiguous",
                        message=(
                            f"Anchor {file}:{target_line} matched {len(candidates)} definitions; "
                            "choosing innermost"
                        ),
                    )
                )
            if target_col is not None:
                filtered = [
                    record
                    for record in candidates
                    if (target_line > record.start_line or target_col >= record.start_col)
                    and (target_line < record.end_line or target_col <= record.end_col)
                ]
                if filtered:
                    candidates = filtered
            return candidates[0], tuple(degrades)

        degrades.append(
            DegradeEventV1(
                stage="structural.target_resolution",
                severity="warning",
                category="not_found",
                message=f"No anchor match for {file}:{target_line}; trying name-based resolution",
            )
        )

    name_candidates = [
        record
        for record in snapshot.def_records
        if _normalize_file_path(record.file) == target_file and _extract_name(record) == name
    ]
    if name_candidates:
        name_candidates.sort(key=_record_sort_key)
        if len(name_candidates) > 1:
            degrades.append(
                DegradeEventV1(
                    stage="structural.target_resolution",
                    severity="warning",
                    category="ambiguous",
                    message=(
                        f"Found {len(name_candidates)} definitions for '{name}' in {file}; "
                        "choosing deterministic first match"
                    ),
                )
            )
        return name_candidates[0], tuple(degrades)

    return None, tuple(degrades)


def _collect_neighborhood(target: SgRecord, snapshot: ScanSnapshot) -> StructuralNeighborhood:
    from tools.cq.utils.interval_index import IntervalIndex

    interval_index: IntervalIndex[SgRecord] | None = (
        snapshot.interval_index if isinstance(snapshot.interval_index, IntervalIndex) else None
    )

    target_file = _normalize_file_path(target.file)
    parents: list[SgRecord] = []
    if interval_index is not None:
        candidates = interval_index.find_candidates(target.start_line)
        parents = [
            candidate
            for candidate in candidates
            if candidate != target
            and _normalize_file_path(candidate.file) == target_file
            and candidate.start_line <= target.start_line <= candidate.end_line
        ]

    enclosing_context: SgRecord | None = None
    if parents:
        enclosing_context = min(parents, key=_anchor_sort_key)

    children = [
        def_rec
        for def_rec in snapshot.def_records
        if _normalize_file_path(def_rec.file) == target_file
        and def_rec != target
        and target.start_line <= def_rec.start_line <= target.end_line
    ]

    if enclosing_context is not None:
        siblings = [
            def_rec
            for def_rec in snapshot.def_records
            if _normalize_file_path(def_rec.file) == target_file
            and def_rec != target
            and enclosing_context.start_line <= def_rec.start_line <= enclosing_context.end_line
            and def_rec not in children
        ]
    else:
        siblings = [
            def_rec
            for def_rec in snapshot.def_records
            if _normalize_file_path(def_rec.file) == target_file
            and def_rec != target
            and def_rec not in children
        ]

    target_key = f"{target.file}:{target.start_line}:{target.start_col}"
    callees = list(snapshot.calls_by_def.get(target_key, ()))

    target_name = _extract_name(target)
    callers: list[SgRecord] = []
    for def_rec in snapshot.def_records:
        def_key = f"{def_rec.file}:{def_rec.start_line}:{def_rec.start_col}"
        def_calls = snapshot.calls_by_def.get(def_key, ())
        if any(_extract_name(call) == target_name for call in def_calls):
            callers.append(def_rec)

    parents.sort(key=_record_sort_key)
    children.sort(key=_record_sort_key)
    siblings.sort(key=_record_sort_key)
    callees.sort(key=_record_sort_key)
    callers.sort(key=_record_sort_key)

    return StructuralNeighborhood(
        parents=tuple(parents),
        children=tuple(children),
        siblings=tuple(siblings),
        enclosing_context=enclosing_context,
        callers=tuple(callers),
        callees=tuple(callees),
    )


def _apply_limit(
    records: tuple[SgRecord, ...],
    kind: str,
    default_limit: int,
    slice_limits: Mapping[str, int] | None,
) -> tuple[SgRecord, ...]:
    limit = max(1, default_limit)
    if slice_limits is not None:
        limit = max(1, int(slice_limits.get(kind, limit)))
    return records[:limit]


def _extract_name(record: SgRecord) -> str:
    text = record.text.strip()
    extracted = text
    if text.startswith(("def ", "async def ", "class ")):
        if text.startswith("async def "):
            text = text[len("async ") :]
        parts = text.split("(", 1)
        if parts:
            extracted = parts[0].split()[-1]
    elif text.startswith(("pub fn ", "fn ")):
        head = text.split("(", 1)[0].strip()
        if head.startswith("pub fn "):
            extracted = head[len("pub fn ") :].strip()
        elif head.startswith("fn "):
            extracted = head[len("fn ") :].strip()
    elif text.startswith(("struct ", "enum ", "trait ", "impl ")):
        head = text.split("{", 1)[0].strip()
        extracted = head.split()[-1]
    elif "(" in text:
        call_part = text.split("(", 1)[0]
        extracted = call_part.split(".")[-1] if "." in call_part else call_part.strip()
    return extracted


def _normalize_file_path(path: str) -> str:
    normalized = path.strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return Path(normalized).as_posix()


def _node_id_from_record(record: SgRecord) -> str:
    return f"structural.{record.kind}.{record.file}:{record.start_line}:{record.start_col}"


def _record_to_node_ref(record: SgRecord) -> SemanticNodeRefV1:
    name = _extract_name(record)
    return SemanticNodeRefV1(
        node_id=_node_id_from_record(record),
        kind=record.kind,
        name=name,
        display_label=name,
        file_path=record.file,
        byte_span=None,
    )


def _record_sort_key(record: SgRecord) -> tuple[str, int, int, str]:
    return (record.file, record.start_line, record.start_col, record.text)


def _anchor_sort_key(record: SgRecord) -> tuple[int, int, int, str]:
    return (
        max(0, record.end_line - record.start_line),
        record.start_line,
        record.start_col,
        record.text,
    )


__all__ = [
    "StructuralNeighborhood",
    "StructuralNeighborhoodCollectRequest",
    "collect_structural_neighborhood",
]
