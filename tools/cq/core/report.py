"""Markdown report renderer for cq results."""

from __future__ import annotations

from tools.cq.core.schema import Artifact, CqResult, Finding, Section

# Maximum evidence items to show before truncating
MAX_EVIDENCE_DISPLAY = 20
MAX_SECTION_FINDINGS = 50


def _severity_icon(severity: str) -> str:
    """Return icon for severity level.

    Returns
    -------
    str
        Severity icon prefix.
    """
    return {
        "error": "[!]",
        "warning": "[~]",
        "info": "",
    }.get(severity, "")


def _format_finding(f: Finding, *, show_anchor: bool = True) -> str:
    """Format a single finding as a markdown line.

    Parameters
    ----------
    f : Finding
        Finding to format.
    show_anchor : bool
        Whether to include source location.

    Returns
    -------
    str
        Markdown-formatted line.
    """
    icon = _severity_icon(f.severity)
    prefix = f"{icon} " if icon else ""

    if show_anchor and f.anchor:
        loc = f"`{f.anchor.to_ref()}`"
        return f"- {prefix}{f.message} ({loc})"
    return f"- {prefix}{f.message}"


def _format_section(s: Section) -> str:
    """Format a section with its findings.

    Parameters
    ----------
    s : Section
        Section to format.

    Returns
    -------
    str
        Markdown-formatted section.
    """
    lines = [f"### {s.title}"]

    if not s.findings:
        lines.append("_No findings_")
        return "\n".join(lines)

    displayed = s.findings[:MAX_SECTION_FINDINGS]
    lines.extend([_format_finding(finding) for finding in displayed])

    remaining = len(s.findings) - len(displayed)
    if remaining > 0:
        lines.append(f"\n_... and {remaining} more_")

    return "\n".join(lines)


def _render_summary(summary: dict[str, object]) -> list[str]:
    """Render summary section lines.

    Returns
    -------
    list[str]
        Summary section lines.
    """
    if not summary:
        return []
    lines = ["## Summary"]
    lines.extend([f"- **{key}**: {value}" for key, value in summary.items()])
    lines.append("")
    return lines


def _render_key_findings(findings: list[Finding]) -> list[str]:
    """Render key findings section lines.

    Returns
    -------
    list[str]
        Key findings section lines.
    """
    if not findings:
        return []
    lines = ["## Key Findings"]
    lines.extend([_format_finding(finding) for finding in findings])
    lines.append("")
    return lines


def _render_sections(sections: list[Section]) -> list[str]:
    """Render section blocks.

    Returns
    -------
    list[str]
        Rendered section lines.
    """
    lines: list[str] = []
    for section in sections:
        lines.append(_format_section(section))
        lines.append("")
    return lines


def _render_evidence(findings: list[Finding]) -> list[str]:
    """Render evidence section lines.

    Returns
    -------
    list[str]
        Evidence section lines.
    """
    if not findings:
        return []
    lines = ["## Evidence"]
    displayed = findings[:MAX_EVIDENCE_DISPLAY]
    lines.extend([_format_finding(finding) for finding in displayed])
    remaining = len(findings) - len(displayed)
    if remaining > 0:
        lines.append(f"\n_... and {remaining} more evidence items_")
    lines.append("")
    return lines


def _render_artifacts(artifacts: list[Artifact]) -> list[str]:
    """Render artifact section lines.

    Returns
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

    Returns
    -------
    list[str]
        Footer lines.
    """
    footer = f"_Completed in {result.run.elapsed_ms:.0f}ms | Schema: {result.run.schema_version}_"
    return ["---", footer]


def render_markdown(result: CqResult) -> str:
    """Render CqResult as markdown for Claude Code context.

    Parameters
    ----------
    result : CqResult
        Analysis result to render.

    Returns
    -------
    str
        Markdown-formatted report.
    """
    lines = [f"# cq {result.run.macro}", ""]
    lines.extend(_render_summary(result.summary))
    lines.extend(_render_key_findings(result.key_findings))
    lines.extend(_render_sections(result.sections))
    lines.extend(_render_evidence(result.evidence))
    lines.extend(_render_artifacts(result.artifacts))
    lines.extend(_render_footer(result))
    return "\n".join(lines)
