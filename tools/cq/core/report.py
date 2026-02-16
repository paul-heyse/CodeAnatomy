"""Markdown report renderer for cq results."""

from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.ports import RenderEnrichmentPort
from tools.cq.core.render_diagnostics import (
    summary_with_render_enrichment_metrics as _summary_with_render_enrichment_metrics,
)
from tools.cq.core.render_enrichment import (
    extract_enrichment_payload as _extract_enrichment_payload,
)
from tools.cq.core.render_enrichment import (
    format_enrichment_facts as _format_enrichment_facts,
)
from tools.cq.core.render_enrichment_orchestrator import (
    count_render_enrichment_tasks as _count_render_enrichment_tasks_orchestrator,
)
from tools.cq.core.render_enrichment_orchestrator import (
    maybe_attach_render_enrichment as _maybe_attach_render_enrichment_orchestrator,
)
from tools.cq.core.render_enrichment_orchestrator import (
    precompute_render_enrichment_cache as _precompute_render_enrichment_cache_orchestrator,
)
from tools.cq.core.render_enrichment_orchestrator import (
    select_enrichment_target_files as _select_enrichment_target_files_orchestrator,
)
from tools.cq.core.render_overview import render_code_overview as _render_code_overview
from tools.cq.core.render_summary import (
    compact_summary_for_rendering,
    render_summary_condensed,
)
from tools.cq.core.render_summary import (
    render_insight_card_from_summary as _render_insight_card_from_summary,
)
from tools.cq.core.render_summary import (
    render_summary as _render_summary,
)
from tools.cq.core.render_utils import clean_scalar as _clean_scalar
from tools.cq.core.render_utils import format_location as _format_location
from tools.cq.core.render_utils import safe_int as _safe_int
from tools.cq.core.schema import Artifact, CqResult, Finding, Section

# Maximum evidence items to show before truncating
MAX_EVIDENCE_DISPLAY = 20
MAX_SECTION_FINDINGS = 50
SHOW_CONTEXT_SNIPPETS_ENV = "CQ_RENDER_CONTEXT_SNIPPETS"
MAX_OBJECT_OCCURRENCE_LINES = 200
_RENDER_ENRICHMENT_PORT_STATE: dict[str, RenderEnrichmentPort | None] = {"port": None}

# Section ordering per front-door command
_SECTION_ORDER_MAP: dict[str, tuple[str, ...]] = {
    "search": (
        "Target Candidates",
        "Neighborhood Preview",
        "Resolved Objects",
        "Occurrences",
        "Uses by Kind",
        "Non-Code Matches (Strings / Comments / Docstrings)",
        "Hot Files",
        "Suggested Follow-ups",
        "Cross-Language Diagnostics",
    ),
    "calls": (
        "Neighborhood Preview",
        "Target Callees",
        "Argument Shape Histogram",
        "Hazards",
        "Keyword Argument Usage",
        "Calling Contexts",
        "Call Sites",
    ),
}


def set_render_enrichment_port(port: RenderEnrichmentPort | None) -> None:
    """Set render enrichment provider for anchor-based payload generation."""
    _RENDER_ENRICHMENT_PORT_STATE["port"] = port


def _severity_icon(severity: str) -> str:
    """Return icon for severity level.

    Returns:
    -------
    str
        Severity icon prefix.
    """
    return {
        "error": "[!]",
        "warning": "[~]",
        "info": "",
    }.get(severity, "")


def _format_finding(
    f: Finding,
    *,
    show_anchor: bool = True,
    show_context: bool = True,
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
) -> str:
    """Format a single finding as a markdown line.

    Parameters
    ----------
    f : Finding
        Finding to format.
    show_anchor : bool
        Whether to include source location.

    Returns:
    -------
    str
        Markdown-formatted line(s), including context snippet if available.
    """
    if root is not None:
        _maybe_attach_render_enrichment_orchestrator(
            f,
            root=root,
            cache=enrich_cache,
            allowed_files=allowed_enrichment_files,
            port=_RENDER_ENRICHMENT_PORT_STATE["port"],
        )

    rendered_lines = [_format_finding_base_line(f, show_anchor=show_anchor)]

    enrichment_payload = _extract_enrichment_payload(f)
    if enrichment_payload is not None:
        rendered_lines.extend(_format_enrichment_facts(enrichment_payload))
    rendered_lines.extend(_format_resolved_object_occurrences(f))

    rendered_lines.extend(_format_context_block(f, enabled=show_context))

    return "\n".join(rendered_lines)


def _format_finding_base_line(finding: Finding, *, show_anchor: bool) -> str:
    prefix = _format_finding_prefix(finding)
    if show_anchor and finding.anchor:
        loc = f"`{finding.anchor.to_ref()}`"
        return f"- {prefix}{finding.message} ({loc})"
    return f"- {prefix}{finding.message}"


def _format_finding_prefix(finding: Finding) -> str:
    if "impact_bucket" in finding.details and "confidence_bucket" in finding.details:
        return f"[impact:{finding.details['impact_bucket']}] [conf:{finding.details['confidence_bucket']}] "
    icon = _severity_icon(finding.severity)
    return f"{icon} " if icon else ""


def _format_context_block(finding: Finding, *, enabled: bool = True) -> list[str]:
    if not enabled:
        return []
    if os.environ.get(SHOW_CONTEXT_SNIPPETS_ENV, "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return []
    context_snippet = finding.details.get("context_snippet")
    if not isinstance(context_snippet, str) or not context_snippet:
        return []
    context_window = finding.details.get("context_window")
    if context_window and isinstance(context_window, dict):
        start = context_window.get("start_line", "?")
        end = context_window.get("end_line", "?")
        header = f"  Context (lines {start}-{end}):"
    else:
        header = "  Context:"
    language = finding.details.get("language")
    lang = language if isinstance(language, str) else "python"
    indented_snippet = "\n".join(f"  {line}" for line in context_snippet.split("\n"))
    return [header, f"  ```{lang}", indented_snippet, "  ```"]


def _format_resolved_object_occurrences(finding: Finding) -> list[str]:
    if finding.category != "resolved_object":
        return []
    occurrences = finding.details.get("occurrences")
    if not isinstance(occurrences, list) or not occurrences:
        return []

    lines = ["  Occurrence Locations:"]
    for row in occurrences[:MAX_OBJECT_OCCURRENCE_LINES]:
        if not isinstance(row, dict):
            continue
        line_id = (
            _clean_scalar(row.get("line_id"))
            or _clean_scalar(row.get("occurrence_id"))
            or "unknown"
        )
        file_value = _clean_scalar(row.get("file"))
        line_value = _safe_int(row.get("line"))
        col_value = _safe_int(row.get("col"))
        location = _format_location(file_value, line_value, col_value) or "<unknown>"
        block_ref = _clean_scalar(row.get("block_ref"))
        if block_ref is None:
            start_line = _safe_int(row.get("block_start_line")) or _safe_int(
                row.get("context_start_line")
            )
            end_line = _safe_int(row.get("block_end_line")) or _safe_int(
                row.get("context_end_line")
            )
            if file_value is not None and start_line is not None and end_line is not None:
                block_ref = f"{file_value}:{start_line}-{end_line}"
            elif file_value is not None:
                block_ref = f"{file_value}:?"
            else:
                block_ref = "<unknown>"
        lines.append(f"  - line_id={line_id}: {location} (block {block_ref})")

    remaining = len(occurrences) - MAX_OBJECT_OCCURRENCE_LINES
    if remaining > 0:
        lines.append(f"  - ... +{remaining} more occurrences")
    return lines


def _format_section(
    s: Section,
    *,
    show_context: bool = True,
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
) -> str:
    """Format a section with its findings.

    Parameters
    ----------
    s : Section
        Section to format.

    Returns:
    -------
    str
        Markdown-formatted section.
    """
    lines = [f"### {s.title}"]

    if not s.findings:
        lines.append("_No findings_")
        return "\n".join(lines)

    displayed = s.findings[:MAX_SECTION_FINDINGS]
    lines.extend(
        [
            _format_finding(
                finding,
                show_context=show_context,
                root=root,
                enrich_cache=enrich_cache,
                allowed_enrichment_files=allowed_enrichment_files,
            )
            for finding in displayed
        ]
    )

    remaining = len(s.findings) - len(displayed)
    if remaining > 0:
        lines.append(f"\n_... and {remaining} more_")

    return "\n".join(lines)


def _render_key_findings(
    findings: list[Finding],
    *,
    show_context: bool = True,
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
) -> list[str]:
    """Render key findings section lines.

    Returns:
    -------
    list[str]
        Key findings section lines.
    """
    if not findings:
        return []
    lines = ["## Key Findings"]
    lines.extend(
        [
            _format_finding(
                finding,
                show_context=show_context,
                root=root,
                enrich_cache=enrich_cache,
                allowed_enrichment_files=allowed_enrichment_files,
            )
            for finding in findings
        ]
    )
    lines.append("")
    return lines


def _finding_dedupe_key(finding: Finding) -> tuple[object, ...]:
    anchor = finding.anchor
    if anchor is None:
        return (finding.category, finding.message, None, None, None, finding.severity)
    return (
        finding.category,
        finding.message,
        anchor.file,
        anchor.line,
        int(anchor.col or 0),
        finding.severity,
    )


def _render_sections(
    sections: list[Section],
    *,
    show_context: bool = True,
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
    seen_keys: set[tuple[object, ...]] | None = None,
) -> list[str]:
    """Render section blocks.

    Returns:
    -------
    list[str]
        Rendered section lines.
    """
    lines: list[str] = []
    for section in sections:
        findings = section.findings
        if seen_keys is not None:
            deduped: list[Finding] = []
            for finding in findings:
                key = _finding_dedupe_key(finding)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped.append(finding)
            findings = deduped
        if not findings:
            continue
        lines.append(
            _format_section(
                Section(title=section.title, findings=findings, collapsed=section.collapsed),
                show_context=show_context,
                root=root,
                enrich_cache=enrich_cache,
                allowed_enrichment_files=allowed_enrichment_files,
            )
        )
        lines.append("")
    return lines


def _render_evidence(
    findings: list[Finding],
    *,
    show_context: bool = True,
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
    seen_keys: set[tuple[object, ...]] | None = None,
) -> list[str]:
    """Render evidence section lines.

    Returns:
    -------
    list[str]
        Evidence section lines.
    """
    if not findings:
        return []
    lines = ["## Evidence"]
    if seen_keys is not None:
        deduped: list[Finding] = []
        for finding in findings:
            key = _finding_dedupe_key(finding)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(finding)
        findings = deduped
    displayed = findings[:MAX_EVIDENCE_DISPLAY]
    lines.extend(
        [
            _format_finding(
                finding,
                show_context=show_context,
                root=root,
                enrich_cache=enrich_cache,
                allowed_enrichment_files=allowed_enrichment_files,
            )
            for finding in displayed
        ]
    )
    remaining = len(findings) - len(displayed)
    if remaining > 0:
        lines.append(f"\n_... and {remaining} more evidence items_")
    lines.append("")
    return lines


def _render_artifacts(artifacts: list[Artifact]) -> list[str]:
    """Render artifact section lines.

    Returns:
    -------
    list[str]
        Artifact section lines.
    """
    if not artifacts:
        return []
    lines = ["## Artifacts"]
    lines.extend([f"- `{artifact.path}` ({artifact.format})" for artifact in artifacts])
    lines.append("")
    return lines


def _render_footer(result: CqResult) -> list[str]:
    """Render report footer lines.

    Returns:
    -------
    list[str]
        Footer lines.
    """
    footer = f"_Completed in {result.run.elapsed_ms:.0f}ms | Schema: {result.run.schema_version}_"
    return ["---", footer]


def _reorder_sections(sections: list[Section], macro: str) -> list[Section]:
    """Reorder sections according to fixed order for the command.

    Sections not in the order map are appended at end.

    Parameters
    ----------
    sections : list[Section]
        Sections to reorder.
    macro : str
        Command name (e.g. ``search``, ``calls``).

    Returns:
    -------
    list[Section]
        Reordered sections.
    """
    order = _SECTION_ORDER_MAP.get(macro)
    if order is None:
        return sections
    order_index = {title: idx for idx, title in enumerate(order)}
    known = [s for s in sections if s.title in order_index]
    unknown = [s for s in sections if s.title not in order_index]
    known.sort(key=lambda s: order_index[s.title])
    return known + unknown


def render_markdown(result: CqResult) -> str:
    """Render CqResult as markdown for Claude Code context.

    Parameters
    ----------
    result : CqResult
        Analysis result to render.

    Returns:
    -------
    str
        Markdown-formatted report.
    """
    root = Path(result.run.root)
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] = {}
    allowed_enrichment_files = _select_enrichment_target_files_orchestrator(result)
    all_task_count = _count_render_enrichment_tasks_orchestrator(
        result=result,
        root=root,
        allowed_files=None,
    )
    rendered_tasks = _precompute_render_enrichment_cache_orchestrator(
        result=result,
        root=root,
        cache=enrich_cache,
        allowed_files=allowed_enrichment_files,
        port=_RENDER_ENRICHMENT_PORT_STATE["port"],
    )
    applied = sum(
        1
        for task in rendered_tasks
        if enrich_cache.get((task.file, task.line, task.col, task.language))
    )
    attempted = len(rendered_tasks)
    failed = max(0, attempted - applied)
    skipped = max(0, all_task_count - attempted)
    summary_with_metrics = _summary_with_render_enrichment_metrics(
        result.summary,
        attempted=attempted,
        applied=applied,
        failed=failed,
        skipped=skipped,
    )
    rendered_seen_keys = {_finding_dedupe_key(finding) for finding in result.key_findings}

    # Apply compact diagnostics
    compact_summary, _offloaded = compact_summary_for_rendering(summary_with_metrics)

    lines = [f"# cq {result.run.macro}", ""]
    lines.extend(_render_insight_card_from_summary(result.summary))
    lines.extend(_render_code_overview(result))
    lines.extend(
        _render_key_findings(
            result.key_findings,
            show_context=result.run.macro != "search",
            root=root,
            enrich_cache=enrich_cache,
            allowed_enrichment_files=allowed_enrichment_files,
        )
    )
    reordered = _reorder_sections(result.sections, result.run.macro)
    lines.extend(
        _render_sections(
            reordered,
            show_context=result.run.macro != "search",
            root=root,
            enrich_cache=enrich_cache,
            allowed_enrichment_files=allowed_enrichment_files,
            seen_keys=rendered_seen_keys,
        )
    )
    lines.extend(
        _render_evidence(
            result.evidence,
            show_context=result.run.macro != "search",
            root=root,
            enrich_cache=enrich_cache,
            allowed_enrichment_files=allowed_enrichment_files,
            seen_keys=rendered_seen_keys,
        )
    )
    lines.extend(_render_artifacts(result.artifacts))
    if result.run.macro != "search":
        lines.extend(_render_summary(compact_summary))
    lines.extend(_render_footer(result))
    return "\n".join(lines)


# Public API re-exports
render_summary_compact = render_summary_condensed

from tools.cq.core.render_summary import ARTIFACT_ONLY_KEYS

__all__ = [
    "ARTIFACT_ONLY_KEYS",
    "compact_summary_for_rendering",
    "render_markdown",
    "render_summary_compact",
]
