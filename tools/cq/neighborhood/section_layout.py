"""Deterministic section layout for semantic neighborhood bundles.

This module provides deterministic section ordering via fixed SECTION_ORDER.
Assembly uses a slot map keyed by slice kind, then emits sections in fixed order
to prevent drift from collector return order.
"""

from __future__ import annotations

from tools.cq.core.snb_schema import (
    DegradeEventV1,
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)
from tools.cq.core.structs import CqStruct

SECTION_ORDER: tuple[str, ...] = (
    "target_tldr",  # 01 - collapsed: False
    "neighborhood_summary",  # 02 - collapsed: False
    "enclosing_context",  # 03 - collapsed: dynamic
    "parents",  # 04 - collapsed: dynamic
    "children",  # 05 - collapsed: True
    "siblings",  # 06 - collapsed: True
    "callers",  # 07 - collapsed: True
    "callees",  # 08 - collapsed: True
    "references",  # 09 - collapsed: True
    "implementations",  # 10 - collapsed: True
    "type_supertypes",  # 11 - collapsed: True
    "type_subtypes",  # 12 - collapsed: True
    "imports",  # 13 - collapsed: True
    "semantic_deep_signals",  # 14 - collapsed: True
    "diagnostics",  # 15 - collapsed: True
    "suggested_followups",  # 16 - collapsed: False
    "provenance",  # 17 - collapsed: True
)

_UNCOLLAPSED_SECTIONS = frozenset(
    {
        "target_tldr",
        "neighborhood_summary",
        "suggested_followups",
    }
)

_DYNAMIC_COLLAPSE_SECTIONS: dict[str, int] = {
    "parents": 3,
    "enclosing_context": 1,
}


def is_section_collapsed(kind: str, total: int) -> bool:
    """Return whether a section kind should default to collapsed.

    Parameters
    ----------
    kind : str
        Section kind identifier.
    total : int
        Total number of items in the section.

    Returns:
    -------
    bool
        True if section should be collapsed by default.
    """
    if kind in _UNCOLLAPSED_SECTIONS:
        return False
    threshold = _DYNAMIC_COLLAPSE_SECTIONS.get(kind)
    if threshold is None:
        return True
    return total > threshold


class SectionV1(CqStruct, frozen=True):
    """A single section in the bundle view.

    Parameters
    ----------
    kind : str
        Section kind identifier.
    title : str
        Section display title.
    items : tuple[str, ...]
        Section item lines.
    collapsed : bool
        Whether section is collapsed by default.
    metadata : dict[str, object] | None
        Optional section metadata.
    """

    kind: str
    title: str
    items: tuple[str, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None


class FindingV1(CqStruct, frozen=True):
    """A key finding entry for quick reference.

    Parameters
    ----------
    category : str
        Finding category (e.g., "target", "scope", "signature").
    label : str
        Finding display label.
    value : str
        Finding value.
    """

    category: str
    label: str
    value: str


class BundleViewV1(CqStruct, frozen=True):
    """Complete bundle view with deterministic section ordering.

    Parameters
    ----------
    key_findings : tuple[FindingV1, ...]
        Key findings for quick reference.
    sections : tuple[SectionV1, ...]
        Ordered sections.
    """

    key_findings: tuple[FindingV1, ...] = ()
    sections: tuple[SectionV1, ...] = ()


def materialize_section_layout(
    bundle: SemanticNeighborhoodBundleV1,
) -> BundleViewV1:
    """Convert bundle into ordered Section/Finding layout.

    CRITICAL: Assembly is keyed by slice kind, then emitted in fixed
    SECTION_ORDER. This prevents drift by collector return order.

    Parameters
    ----------
    bundle : SemanticNeighborhoodBundleV1
        Source bundle.

    Returns:
    -------
    BundleViewV1
        View with deterministic section ordering.
    """
    # Build slot map: kind → slice
    slot_map: dict[str, NeighborhoodSliceV1] = {}
    for s in bundle.slices:
        slot_map[s.kind] = s

    # Collect sections in fixed order
    sections: list[SectionV1] = []
    key_findings: list[FindingV1] = []

    # 01: Target TL;DR (always first)
    if bundle.subject is not None:
        key_findings.extend(_build_target_findings(bundle.subject))

    # 02: Neighborhood summary (synthetic section)
    sections.append(_build_summary_section(bundle))

    # Synthetic section kinds (handled separately)
    synthetic_kinds = {
        "target_tldr",
        "neighborhood_summary",
        "suggested_followups",
        "provenance",
        "diagnostics",
    }

    # Emit slice-backed sections in SECTION_ORDER
    for slot_kind in SECTION_ORDER:
        if slot_kind in synthetic_kinds:
            continue  # Synthetic sections handled separately

        if slot_kind in slot_map:
            sections.append(_slice_to_section(slot_map[slot_kind]))

    # Fallback: any slice kinds NOT in SECTION_ORDER go at end
    known_kinds = set(SECTION_ORDER)
    sections.extend(_slice_to_section(s) for s in bundle.slices if s.kind not in known_kinds)

    # Diagnostics section (from typed DegradeEventV1)
    if bundle.diagnostics:
        sections.append(_build_diagnostics_section(bundle.diagnostics))

    # Suggested follow-ups + provenance (always last)
    sections.append(_build_followup_section(bundle))
    sections.append(_build_provenance_section(bundle))

    return BundleViewV1(
        key_findings=tuple(key_findings),
        sections=tuple(sections),
    )


def _build_target_findings(subject: SemanticNodeRefV1) -> list[FindingV1]:
    """Build target TL;DR findings from subject node.

    Parameters
    ----------
    subject : SemanticNodeRefV1
        Subject node reference.

    Returns:
    -------
    list[FindingV1]
        Target findings.
    """
    # Basic identity
    findings: list[FindingV1] = [
        FindingV1(
            category="target",
            label="Symbol",
            value=subject.display_label or subject.name,
        )
    ]

    if subject.kind:
        findings.append(
            FindingV1(
                category="target",
                label="Kind",
                value=subject.kind,
            )
        )

    if subject.file_path:
        findings.append(
            FindingV1(
                category="target",
                label="File",
                value=subject.file_path,
            )
        )

    if subject.signature:
        findings.append(
            FindingV1(
                category="target",
                label="Signature",
                value=subject.signature,
            )
        )

    if subject.qualname:
        findings.append(
            FindingV1(
                category="target",
                label="Qualified Name",
                value=subject.qualname,
            )
        )

    return findings


def _build_summary_section(bundle: SemanticNeighborhoodBundleV1) -> SectionV1:
    """Build neighborhood summary section.

    Parameters
    ----------
    bundle : SemanticNeighborhoodBundleV1
        Source bundle.

    Returns:
    -------
    SectionV1
        Summary section.
    """
    items: list[str] = []

    # Graph statistics
    if bundle.graph is not None:
        items.append(f"**Nodes:** {bundle.graph.node_count}")
        items.append(f"**Edges:** {bundle.graph.edge_count}")

    # Slice summary
    if bundle.slices:
        items.append(f"**Slices:** {len(bundle.slices)}")
        slice_summary = ", ".join(f"{s.kind} ({s.total})" for s in bundle.slices)
        items.append(f"**Relationships:** {slice_summary}")

    # Diagnostics summary
    if bundle.diagnostics:
        error_count = sum(1 for d in bundle.diagnostics if d.severity == "error")
        warning_count = sum(1 for d in bundle.diagnostics if d.severity == "warning")
        info_count = sum(1 for d in bundle.diagnostics if d.severity == "info")

        diag_parts: list[str] = []
        if error_count:
            diag_parts.append(f"{error_count} errors")
        if warning_count:
            diag_parts.append(f"{warning_count} warnings")
        if info_count:
            diag_parts.append(f"{info_count} info")

        if diag_parts:
            items.append(f"**Diagnostics:** {', '.join(diag_parts)}")

    return SectionV1(
        kind="neighborhood_summary",
        title="Neighborhood Summary",
        items=tuple(items),
        collapsed=False,
    )


def _slice_to_section(slice_: NeighborhoodSliceV1) -> SectionV1:
    """Convert neighborhood slice to section.

    Parameters
    ----------
    slice_ : NeighborhoodSliceV1
        Source slice.

    Returns:
    -------
    SectionV1
        Section with formatted items.
    """
    items: list[str] = []

    # Preview nodes
    for node in slice_.preview:
        display = node.display_label or node.name
        if node.file_path:
            items.append(f"- **{display}** ({node.file_path})")
        else:
            items.append(f"- **{display}**")

    # Overflow indicator
    if slice_.total > len(slice_.preview):
        overflow = slice_.total - len(slice_.preview)
        items.append(f"_... and {overflow} more_")

    # Determine collapse state
    collapsed = is_section_collapsed(slice_.kind, slice_.total)

    return SectionV1(
        kind=slice_.kind,
        title=slice_.title,
        items=tuple(items),
        collapsed=collapsed,
        metadata=slice_.metadata,
    )


def _build_diagnostics_section(
    diagnostics: tuple[DegradeEventV1, ...],
) -> SectionV1:
    """Build diagnostics section from typed degrade events.

    Parameters
    ----------
    diagnostics : tuple[DegradeEventV1, ...]
        Degradation events.

    Returns:
    -------
    SectionV1
        Diagnostics section.
    """
    items: list[str] = []

    for event in diagnostics:
        severity_marker = {
            "error": "❌",
            "warning": "⚠️",
            "info": "ⓘ",  # Using circled i instead of ambiguous information source
        }.get(event.severity, "•")

        item = f"{severity_marker} **{event.stage}** ({event.category})"
        if event.message:
            item += f": {event.message}"

        items.append(item)

    return SectionV1(
        kind="diagnostics",
        title="Diagnostics",
        items=tuple(items),
        collapsed=True,
    )


_HIGH_COUNT_THRESHOLD = 10


def _build_followup_section(bundle: SemanticNeighborhoodBundleV1) -> SectionV1:
    """Build suggested follow-ups section.

    Parameters
    ----------
    bundle : SemanticNeighborhoodBundleV1
        Source bundle.

    Returns:
    -------
    SectionV1
        Follow-ups section.
    """
    items: list[str] = []

    # Suggest exploring high-count slices
    high_count_slices = [s for s in bundle.slices if s.total > _HIGH_COUNT_THRESHOLD]
    if high_count_slices:
        items.extend(
            [
                "**Explore high-count relationships:**",
                *[f"- {s.title}: {s.total} items" for s in high_count_slices[:3]],
            ]
        )

    # Suggest investigating diagnostics
    if bundle.diagnostics:
        errors = [d for d in bundle.diagnostics if d.severity == "error"]
        if errors:
            items.extend(
                [
                    "**Investigate errors:**",
                    *[f"- {err.stage}: {err.message}" for err in errors[:3]],
                ]
            )

    if not items:
        items.append("No specific follow-ups suggested.")

    return SectionV1(
        kind="suggested_followups",
        title="Suggested Follow-ups",
        items=tuple(items),
        collapsed=False,
    )


def _build_provenance_section(bundle: SemanticNeighborhoodBundleV1) -> SectionV1:
    """Build provenance section from bundle metadata.

    Parameters
    ----------
    bundle : SemanticNeighborhoodBundleV1
        Source bundle.

    Returns:
    -------
    SectionV1
        Provenance section.
    """
    items: list[str] = []

    if bundle.meta is not None:
        items.append(f"**Tool:** {bundle.meta.tool}")

        if bundle.meta.tool_version:
            items.append(f"**Version:** {bundle.meta.tool_version}")

        if bundle.meta.workspace_root:
            items.append(f"**Workspace:** {bundle.meta.workspace_root}")

        if bundle.meta.created_at_ms is not None:
            items.append(f"**Created:** {bundle.meta.created_at_ms}ms")

        if bundle.meta.semantic_sources:
            items.append(f"**Semantic Sources:** {len(bundle.meta.semantic_sources)} configured")

    items.append(f"**Schema:** {bundle.schema_version}")
    items.append(f"**Bundle ID:** {bundle.bundle_id}")

    if bundle.artifacts:
        items.append(f"**Artifacts:** {len(bundle.artifacts)} stored")

    return SectionV1(
        kind="provenance",
        title="Provenance",
        items=tuple(items),
        collapsed=True,
    )


__all__ = [
    "SECTION_ORDER",
    "BundleViewV1",
    "FindingV1",
    "SectionV1",
    "is_section_collapsed",
    "materialize_section_layout",
]
