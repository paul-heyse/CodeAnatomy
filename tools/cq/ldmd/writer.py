"""LDMD writer with preview/body separation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.snb_schema import (
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
)
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult
    from tools.cq.core.summary_contract import CqSummary


def render_ldmd_document(
    bundle: SemanticNeighborhoodBundleV1,
    *,
    include_manifest: bool = True,
) -> str:
    """Render SNB bundle as LDMD-marked markdown.

    Preview/body separation:
    - TLDR block: slice summary + top-K items (preview)
    - Body block: remaining items + artifact refs
    - Manifest: section count, byte sizes, artifact pointers

    Parameters
    ----------
    bundle
        Expected to be SemanticNeighborhoodBundleV1 (from R1).
        Currently accepts object type since R1 schema doesn't exist yet.
    include_manifest
        Whether to include provenance manifest section.

    Returns:
    -------
    str
        LDMD-marked markdown document.
    """
    lines: list[str] = []
    _emit_snb_target(lines, bundle)
    _emit_snb_summary(lines, bundle)
    _emit_snb_slices(lines, bundle)
    _emit_snb_diagnostics(lines, bundle)
    if include_manifest:
        _emit_snb_provenance(lines, bundle)
    return "\n".join(lines)


# -- SNB bundle LDMD helpers -------------------------------------------------


def _emit_snb_target(lines: list[str], bundle: SemanticNeighborhoodBundleV1) -> None:
    subject = bundle.subject
    subject_name = subject.name if subject is not None else "<unknown>"
    subject_file = subject.file_path if subject is not None else ""
    subject_span = subject.byte_span if subject is not None else None

    lines.append(_ldmd_begin("target_tldr", title="Target", level=1))
    lines.append(f"## Target: {subject_name}")
    if subject_file:
        lines.append(f"**File:** {subject_file}")
    if subject_span is not None:
        lines.append(f"**Byte span:** {subject_span[0]}..{subject_span[1]}")
    lines.append(_ldmd_end("target_tldr"))
    lines.append("")


def _emit_snb_summary(lines: list[str], bundle: SemanticNeighborhoodBundleV1) -> None:
    graph = bundle.graph
    node_count = graph.node_count if graph is not None else 0
    edge_count = graph.edge_count if graph is not None else 0

    lines.append(_ldmd_begin("neighborhood_summary", title="Neighborhood Summary", level=1))
    lines.append("## Neighborhood Summary")
    lines.append(f"- **Structural nodes:** {node_count}")
    lines.append(f"- **Relationships:** {edge_count}")
    lines.append(_ldmd_end("neighborhood_summary"))
    lines.append("")


def _emit_snb_slices(lines: list[str], bundle: SemanticNeighborhoodBundleV1) -> None:
    for s in bundle.slices:
        _emit_snb_slice(lines, s)


def _emit_snb_slice(lines: list[str], s: NeighborhoodSliceV1) -> None:
    kind = s.kind
    total = s.total
    preview = s.preview

    lines.append(_ldmd_begin(kind, title=kind.replace("_", " ").title(), level=2))
    lines.append(f"### {kind.replace('_', ' ').title()} ({total})")

    if preview:
        lines.append(_ldmd_begin(f"{kind}_tldr", parent=kind, level=3))
        for i, node in enumerate(preview, 1):
            lines.append(f"{i}. {node.name} ({node.kind})")
        lines.append(_ldmd_end(f"{kind}_tldr"))

    lines.append(_ldmd_begin(f"{kind}_body", parent=kind, level=3))
    if total > len(preview):
        lines.append(f"*Full data: see artifact `slice_{kind}.json` ({total} items)*")
    else:
        for node in preview:
            node_file = node.file_path
            lines.append(f"- **{node.name}** ({node.kind})")
            if node_file:
                lines.append(f"  - {node_file}")
    lines.append(_ldmd_end(f"{kind}_body"))
    lines.append(_ldmd_end(kind))
    lines.append("")


def _emit_snb_diagnostics(lines: list[str], bundle: SemanticNeighborhoodBundleV1) -> None:
    diagnostics = bundle.diagnostics
    if not diagnostics:
        return
    lines.append(_ldmd_begin("diagnostics", title="Diagnostics", level=1))
    lines.append("## Diagnostics")
    lines.extend(f"- **{diag.stage}:** {diag.message}" for diag in diagnostics)
    lines.append(_ldmd_end("diagnostics"))
    lines.append("")


def _emit_snb_provenance(lines: list[str], bundle: SemanticNeighborhoodBundleV1) -> None:
    slices = bundle.slices
    lines.append(_ldmd_begin("provenance", title="Provenance", level=1))
    lines.append("## Provenance")
    lines.append(f"- **Sections:** {len(slices)}")

    meta = bundle.meta
    if meta:
        created_at = meta.created_at_ms
        if created_at is not None:
            lines.append(f"- **Created at (ms):** {created_at}")

    artifacts = bundle.artifacts
    if artifacts:
        lines.append("- **Artifacts:**")
        for art in artifacts:
            storage_path = art.storage_path
            byte_size = art.byte_size
            if storage_path:
                lines.append(f"  - `{Path(storage_path).name}` ({byte_size} bytes)")
            else:
                artifact_id = art.artifact_id
                lines.append(f"  - `{artifact_id}` ({byte_size} bytes)")
    lines.append(_ldmd_end("provenance"))


def render_ldmd_from_cq_result(result: CqResult) -> str:
    """Render CqResult as LDMD-marked markdown.

    Adapts CqResult's sections/findings structure into LDMD progressive
    disclosure format. Used by the --format ldmd dispatch path.

    Parameters
    ----------
    result
        CQ analysis result to render.

    Returns:
    -------
    str
        LDMD-marked markdown string.
    """
    lines: list[str] = []
    _emit_run_meta(lines, result)
    _emit_insight_card(lines, result)
    _emit_summary(lines, result)
    _emit_key_findings(lines, result)
    _emit_sections(lines, result)
    _emit_artifacts(lines, result)
    return "\n".join(lines)


# -- CqResult LDMD helpers ---------------------------------------------------

_CQ_PREVIEW_LIMIT = 5


def _emit_insight_card(lines: list[str], result: CqResult) -> None:
    """Emit insight card as first LDMD section if present."""
    raw = result.summary.front_door_insight
    if raw is None:
        return

    from tools.cq.core.front_door_render import render_insight_card
    from tools.cq.core.front_door_schema import FrontDoorInsightV1

    insight: FrontDoorInsightV1 | None = None
    if isinstance(raw, FrontDoorInsightV1):
        insight = raw
    elif isinstance(raw, dict):
        try:
            insight = convert_lax(raw, type_=FrontDoorInsightV1)
        except BoundaryDecodeError:
            return
    if insight is None:
        return

    card_lines = render_insight_card(insight)
    lines.append(_ldmd_begin("insight_card", title="Insight Card", level=1))
    lines.extend(card_lines)
    lines.append(_ldmd_end("insight_card"))
    lines.append("")


def _ldmd_begin(
    section_id: str,
    *,
    title: str = "",
    level: int = 0,
    parent: str = "",
) -> str:
    attrs = [f'id="{section_id}"']
    if title:
        attrs.append(f'title="{title}"')
    if level > 0:
        attrs.append(f'level="{level}"')
    if parent:
        attrs.append(f'parent="{parent}"')
    return f"<!--LDMD:BEGIN {' '.join(attrs)}-->"


def _ldmd_end(section_id: str) -> str:
    return f'<!--LDMD:END id="{section_id}"-->'


def _finding_line(finding: object, *, numbered: bool = False, index: int = 0) -> str:
    from tools.cq.core.schema import Anchor, Finding

    if not isinstance(finding, Finding):
        return ""
    anchor_ref = ""
    if isinstance(finding.anchor, Anchor):
        anchor_ref = f" @ {finding.anchor.file}:{finding.anchor.line}"
    prefix = f"{index}." if numbered else "-"
    return f"{prefix} [{finding.severity}] {finding.message}{anchor_ref}"


def _emit_run_meta(lines: list[str], result: CqResult) -> None:
    lines.append(_ldmd_begin("run_meta", title="Run", level=1))
    lines.append(f"## {result.run.macro}")
    lines.append(f"**Root:** {result.run.root}")
    lines.append(f"**Elapsed:** {result.run.elapsed_ms:.0f}ms")
    lines.append(_ldmd_end("run_meta"))
    lines.append("")


def _emit_summary(lines: list[str], result: CqResult) -> None:
    if result.run.macro == "search":
        return
    if not result.summary:
        return
    from tools.cq.core.report import compact_summary_for_rendering

    compact, offloaded = compact_summary_for_rendering(result.summary)
    lines.append(_ldmd_begin("summary", title="Summary", level=1))
    lines.append("## Summary")
    lines.extend(f"- **{key}:** {value}" for key, value in compact.items())
    lines.append(_ldmd_end("summary"))
    lines.append("")

    # Offloaded diagnostics are artifact-only; render only compact key list + refs.
    if offloaded:
        offloaded_keys = ", ".join(key for key, _payload in offloaded)
        refs = _extract_insight_artifact_refs(result.summary)
        lines.append(_ldmd_begin("diagnostic_artifacts", title="Diagnostic Artifacts", level=2))
        lines.append(f"- offloaded_keys: {offloaded_keys}")
        if refs:
            for key, path in refs.items():
                lines.append(f"- {key}: `{path}`")
        else:
            lines.append("- artifact_refs: unavailable")
        lines.append(_ldmd_end("diagnostic_artifacts"))
        lines.append("")


def _extract_insight_artifact_refs(summary: CqSummary) -> dict[str, str]:
    """Extract artifact refs from front_door_insight summary payload.

    Returns:
        Mapping of artifact reference keys to paths.
    """
    from tools.cq.core.front_door_render import coerce_front_door_insight

    raw_insight = summary.front_door_insight
    insight = coerce_front_door_insight(raw_insight)
    if insight is None:
        return {}
    refs: dict[str, str] = {}
    if insight.artifact_refs.diagnostics:
        refs["diagnostics"] = insight.artifact_refs.diagnostics
    if insight.artifact_refs.telemetry:
        refs["telemetry"] = insight.artifact_refs.telemetry
    if insight.artifact_refs.neighborhood_overflow:
        refs["neighborhood_overflow"] = insight.artifact_refs.neighborhood_overflow
    return refs


def _emit_key_findings(lines: list[str], result: CqResult) -> None:
    if not result.key_findings:
        return
    lines.append(_ldmd_begin("key_findings", title="Key Findings", level=1))
    lines.append(f"## Key Findings ({len(result.key_findings)})")

    preview = result.key_findings[:_CQ_PREVIEW_LIMIT]
    lines.append(_ldmd_begin("key_findings_tldr", parent="key_findings", level=2))
    lines.extend(_finding_line(f, numbered=True, index=i) for i, f in enumerate(preview, 1))
    lines.append(_ldmd_end("key_findings_tldr"))

    if len(result.key_findings) > _CQ_PREVIEW_LIMIT:
        lines.append(_ldmd_begin("key_findings_body", parent="key_findings", level=2))
        lines.extend(_finding_line(f) for f in result.key_findings[_CQ_PREVIEW_LIMIT:])
        lines.append(_ldmd_end("key_findings_body"))

    lines.append(_ldmd_end("key_findings"))
    lines.append("")


def _emit_sections(lines: list[str], result: CqResult) -> None:
    for idx, section in enumerate(result.sections):
        sid = f"section_{idx}"
        lines.append(_ldmd_begin(sid, title=section.title, level=1))
        lines.append(f"## {section.title}")

        if section.findings:
            sec_preview = section.findings[:_CQ_PREVIEW_LIMIT]
            lines.append(_ldmd_begin(f"{sid}_tldr", parent=sid, level=2))
            lines.extend(
                _finding_line(f, numbered=True, index=i) for i, f in enumerate(sec_preview, 1)
            )
            lines.append(_ldmd_end(f"{sid}_tldr"))

            if len(section.findings) > _CQ_PREVIEW_LIMIT:
                lines.append(_ldmd_begin(f"{sid}_body", parent=sid, level=2))
                lines.extend(_finding_line(f) for f in section.findings[_CQ_PREVIEW_LIMIT:])
                lines.append(_ldmd_end(f"{sid}_body"))

        lines.append(_ldmd_end(sid))
        lines.append("")


def _emit_artifacts(lines: list[str], result: CqResult) -> None:
    if not result.artifacts:
        return
    lines.append(_ldmd_begin("artifacts", title="Artifacts", level=1))
    lines.append("## Artifacts")
    lines.extend(f"- `{art.path}` ({art.format})" for art in result.artifacts)
    lines.append(_ldmd_end("artifacts"))
    lines.append("")
