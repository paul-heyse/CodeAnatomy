"""Structural neighborhood collector.

Collects structural relationships (parents, children, siblings, callers, callees)
from ast-grep scan snapshot.
"""

from __future__ import annotations

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
    """Structural relationships from ast-grep scan snapshot.

    Parameters
    ----------
    parents : tuple[SgRecord, ...]
        Parent definitions (enclosing scopes).
    children : tuple[SgRecord, ...]
        Child definitions (nested definitions).
    siblings : tuple[SgRecord, ...]
        Sibling definitions (same parent).
    enclosing_context : SgRecord | None
        Immediate enclosing definition.
    callers : tuple[SgRecord, ...]
        Functions/methods that call this definition.
    callees : tuple[SgRecord, ...]
        Functions/methods called by this definition.
    """

    parents: tuple[SgRecord, ...] = ()
    children: tuple[SgRecord, ...] = ()
    siblings: tuple[SgRecord, ...] = ()
    enclosing_context: SgRecord | None = None
    callers: tuple[SgRecord, ...] = ()
    callees: tuple[SgRecord, ...] = ()


def collect_structural_neighborhood(  # noqa: PLR0914
    target_name: str,
    target_file: str,
    snapshot: ScanSnapshot,
    *,
    max_per_slice: int = 50,
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...]]:
    """Collect structural neighborhood slices from scan snapshot.

    Parameters
    ----------
    target_name : str
        Target symbol name.
    target_file : str
        Target file path.
    snapshot : ScanSnapshot
        Scan snapshot with indexed definitions and calls.
    max_per_slice : int
        Maximum nodes per slice (for preview).

    Returns:
    -------
    tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...]]
        (slices, degrade_events).
    """
    target_def = _find_target_definition(target_name, target_file, snapshot)
    if target_def is None:
        degrade = DegradeEventV1(
            stage="structural.target_resolution",
            severity="error",
            category="not_found",
            message=f"Target definition '{target_name}' not found in {target_file}",
        )
        return (), (degrade,)

    # Collect structural relationships
    neighborhood = _collect_neighborhood(target_def, snapshot)

    # Build subject node
    subject_node_id = _node_id_from_record(target_def)

    # Convert to SNB slices
    slices: list[NeighborhoodSliceV1] = []
    degrades: list[DegradeEventV1] = []

    # Parents slice
    if neighborhood.parents:
        parent_nodes = [_record_to_node_ref(r) for r in neighborhood.parents[:max_per_slice]]
        parent_edges = [
            SemanticEdgeV1(
                edge_id=f"{_node_id_from_record(r)}→{subject_node_id}",
                source_node_id=_node_id_from_record(r),
                target_node_id=subject_node_id,
                edge_kind="contains",
                evidence_source="structural.ast",
            )
            for r in neighborhood.parents[:max_per_slice]
        ]
        slices.append(
            NeighborhoodSliceV1(
                kind="parents",
                title="Parent Scopes",
                total=len(neighborhood.parents),
                preview=tuple(parent_nodes),
                edges=tuple(parent_edges),
                collapsed=True,
            )
        )

    # Children slice
    if neighborhood.children:
        child_nodes = [_record_to_node_ref(r) for r in neighborhood.children[:max_per_slice]]
        child_edges = [
            SemanticEdgeV1(
                edge_id=f"{subject_node_id}→{_node_id_from_record(r)}",
                source_node_id=subject_node_id,
                target_node_id=_node_id_from_record(r),
                edge_kind="contains",
                evidence_source="structural.ast",
            )
            for r in neighborhood.children[:max_per_slice]
        ]
        slices.append(
            NeighborhoodSliceV1(
                kind="children",
                title="Nested Definitions",
                total=len(neighborhood.children),
                preview=tuple(child_nodes),
                edges=tuple(child_edges),
                collapsed=True,
            )
        )

    # Siblings slice
    if neighborhood.siblings:
        sibling_nodes = [_record_to_node_ref(r) for r in neighborhood.siblings[:max_per_slice]]
        # Siblings are connected via shared parent (indirect relationship)
        slices.append(
            NeighborhoodSliceV1(
                kind="siblings",
                title="Sibling Definitions",
                total=len(neighborhood.siblings),
                preview=tuple(sibling_nodes),
                edges=(),  # No direct edges for siblings
                collapsed=True,
            )
        )

    # Enclosing context (single parent)
    if neighborhood.enclosing_context is not None:
        enclosing_node = _record_to_node_ref(neighborhood.enclosing_context)
        enclosing_edge = SemanticEdgeV1(
            edge_id=f"{_node_id_from_record(neighborhood.enclosing_context)}→{subject_node_id}",
            source_node_id=_node_id_from_record(neighborhood.enclosing_context),
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

    # Callees slice (functions called by target)
    if neighborhood.callees:
        callee_nodes = [_record_to_node_ref(r) for r in neighborhood.callees[:max_per_slice]]
        callee_edges = [
            SemanticEdgeV1(
                edge_id=f"{subject_node_id}→{_node_id_from_record(r)}",
                source_node_id=subject_node_id,
                target_node_id=_node_id_from_record(r),
                edge_kind="calls",
                evidence_source="structural.ast",
            )
            for r in neighborhood.callees[:max_per_slice]
        ]
        slices.append(
            NeighborhoodSliceV1(
                kind="callees",
                title="Functions Called",
                total=len(neighborhood.callees),
                preview=tuple(callee_nodes),
                edges=tuple(callee_edges),
                collapsed=True,
            )
        )

    # Callers slice (functions that call target)
    if neighborhood.callers:
        caller_nodes = [_record_to_node_ref(r) for r in neighborhood.callers[:max_per_slice]]
        caller_edges = [
            SemanticEdgeV1(
                edge_id=f"{_node_id_from_record(r)}→{subject_node_id}",
                source_node_id=_node_id_from_record(r),
                target_node_id=subject_node_id,
                edge_kind="calls",
                evidence_source="structural.ast",
            )
            for r in neighborhood.callers[:max_per_slice]
        ]
        slices.append(
            NeighborhoodSliceV1(
                kind="callers",
                title="Callers",
                total=len(neighborhood.callers),
                preview=tuple(caller_nodes),
                edges=tuple(caller_edges),
                collapsed=True,
            )
        )

    return tuple(slices), tuple(degrades)


def _find_target_definition(
    name: str,
    file: str,
    snapshot: ScanSnapshot,
) -> SgRecord | None:
    """Find target definition by name and file.

    Parameters
    ----------
    name : str
        Target symbol name.
    file : str
        Target file path.
    snapshot : ScanSnapshot
        Scan snapshot.

    Returns:
    -------
    SgRecord | None
        Target definition record, or None if not found.
    """
    for def_rec in snapshot.def_records:
        if def_rec.file == file and _extract_name(def_rec) == name:
            return def_rec
    return None


def _collect_neighborhood(
    target: SgRecord,
    snapshot: ScanSnapshot,
) -> StructuralNeighborhood:
    """Collect structural relationships for target.

    Parameters
    ----------
    target : SgRecord
        Target definition.
    snapshot : ScanSnapshot
        Scan snapshot.

    Returns:
    -------
    StructuralNeighborhood
        Collected structural relationships.
    """
    # Type-narrow the interval_index
    from tools.cq.utils.interval_index import IntervalIndex

    interval_index: IntervalIndex[SgRecord] | None = (
        snapshot.interval_index if isinstance(snapshot.interval_index, IntervalIndex) else None
    )

    # Parents: all definitions that contain target
    parents: list[SgRecord] = []
    if interval_index is not None:
        candidates = interval_index.find_candidates(target.start_line)
        parents = [c for c in candidates if c != target and c.file == target.file]

    # Enclosing context: immediate parent (not target itself)
    enclosing_context: SgRecord | None = None
    if interval_index is not None:
        candidates = interval_index.find_candidates(target.start_line)
        # Find innermost parent that is NOT target itself
        valid_parents = [c for c in candidates if c != target and c.file == target.file]
        if valid_parents:
            # Innermost is the one with smallest span
            enclosing_context = min(
                valid_parents,
                key=lambda r: (r.end_line - r.start_line, -r.start_line, r.end_line),
            )

    # Children: definitions nested within target
    children: list[SgRecord] = [
        def_rec
        for def_rec in snapshot.def_records
        if def_rec.file == target.file
        and def_rec != target
        and target.start_line <= def_rec.start_line <= target.end_line
    ]

    # Siblings: definitions at same level (share same parent)
    siblings: list[SgRecord] = []
    if enclosing_context is not None:
        # Siblings share same parent but are not nested in target
        siblings = [
            def_rec
            for def_rec in snapshot.def_records
            if def_rec.file == target.file
            and def_rec != target
            and enclosing_context.start_line <= def_rec.start_line <= enclosing_context.end_line
            and def_rec not in children
        ]
    else:
        # Module-level siblings
        siblings = [
            def_rec
            for def_rec in snapshot.def_records
            if def_rec.file == target.file and def_rec != target and def_rec not in children
        ]

    # Callees: calls made by target
    target_key = f"{target.file}:{target.start_line}:{target.start_col}"
    callees_records = snapshot.calls_by_def.get(target_key, ())
    callees: list[SgRecord] = list(callees_records)

    # Callers: definitions that call target
    target_name = _extract_name(target)
    callers: list[SgRecord] = []
    for def_rec in snapshot.def_records:
        def_key = f"{def_rec.file}:{def_rec.start_line}:{def_rec.start_col}"
        def_calls = snapshot.calls_by_def.get(def_key, ())
        # Check if any call within this definition matches target name
        if any(_extract_name(call) == target_name for call in def_calls):
            callers.append(def_rec)

    return StructuralNeighborhood(
        parents=tuple(parents),
        children=tuple(children),
        siblings=tuple(siblings),
        enclosing_context=enclosing_context,
        callers=tuple(callers),
        callees=tuple(callees),
    )


def _extract_name(record: SgRecord) -> str:
    """Extract symbol name from record text.

    Parameters
    ----------
    record : SgRecord
        Record to extract name from.

    Returns:
    -------
    str
        Extracted name.
    """
    # Simple heuristic: extract identifier after def/class/call
    text = record.text.strip()

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

    # Fallback: use text as-is
    return text


def _node_id_from_record(record: SgRecord) -> str:
    """Build node ID from record.

    Parameters
    ----------
    record : SgRecord
        Record to build ID from.

    Returns:
    -------
    str
        Node ID.
    """
    return f"structural.{record.kind}.{record.file}:{record.start_line}:{record.start_col}"


def _record_to_node_ref(record: SgRecord) -> SemanticNodeRefV1:
    """Convert SgRecord to SemanticNodeRefV1.

    Parameters
    ----------
    record : SgRecord
        Record to convert.

    Returns:
    -------
    SemanticNodeRefV1
        Semantic node reference.
    """
    name = _extract_name(record)
    return SemanticNodeRefV1(
        node_id=_node_id_from_record(record),
        kind=record.kind,
        name=name,
        display_label=name,
        file_path=record.file,
        byte_span=None,  # SgRecord uses line/col, not byte offsets
    )


__all__ = [
    "StructuralNeighborhood",
    "collect_structural_neighborhood",
]
