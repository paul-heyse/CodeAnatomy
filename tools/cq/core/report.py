"""Markdown report renderer for cq results."""

from __future__ import annotations

from tools.cq.core.codec import dumps_json_value
from tools.cq.core.schema import Artifact, CqResult, Finding, Section
from tools.cq.core.serialization import to_builtins

# Maximum evidence items to show before truncating
MAX_EVIDENCE_DISPLAY = 20
MAX_SECTION_FINDINGS = 50


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


def _format_finding(f: Finding, *, show_anchor: bool = True) -> str:
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
    # Use impact/confidence tags if available, otherwise fall back to severity icon
    if "impact_bucket" in f.details and "confidence_bucket" in f.details:
        imp_bucket = f.details["impact_bucket"]
        conf_bucket = f.details["confidence_bucket"]
        prefix = f"[impact:{imp_bucket}] [conf:{conf_bucket}] "
    else:
        icon = _severity_icon(f.severity)
        prefix = f"{icon} " if icon else ""

    if show_anchor and f.anchor:
        loc = f"`{f.anchor.to_ref()}`"
        base_line = f"- {prefix}{f.message} ({loc})"
    else:
        base_line = f"- {prefix}{f.message}"

    # Add context snippet if available
    context_snippet = f.details.get("context_snippet")
    context_window = f.details.get("context_window")
    if context_snippet and isinstance(context_snippet, str):
        # Build context header with line range
        if context_window and isinstance(context_window, dict):
            start = context_window.get("start_line", "?")
            end = context_window.get("end_line", "?")
            header = f"  Context (lines {start}-{end}):"
        else:
            header = "  Context:"
        # Indent the code block
        snippet_lines = context_snippet.split("\n")
        indented_snippet = "\n".join(f"  {line}" for line in snippet_lines)
        return f"{base_line}\n{header}\n  ```python\n{indented_snippet}\n  ```"

    return base_line


def _format_section(s: Section) -> str:
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
    lines.extend([_format_finding(finding) for finding in displayed])

    remaining = len(s.findings) - len(displayed)
    if remaining > 0:
        lines.append(f"\n_... and {remaining} more_")

    return "\n".join(lines)


def _render_summary(summary: dict[str, object]) -> list[str]:
    """Render summary section lines.

    Returns:
    -------
    list[str]
        Summary section lines.
    """
    if not summary:
        return []
    lines = ["## Summary"]
    formatted: list[str] = []
    for key, value in summary.items():
        rendered_value = to_builtins(value)
        if isinstance(rendered_value, (dict, list)):
            rendered = dumps_json_value(rendered_value, indent=None)
        else:
            rendered = str(rendered_value)
        formatted.append(f"- **{key}**: {rendered}")
    lines.extend(formatted)
    lines.append("")
    return lines


def _render_key_findings(findings: list[Finding]) -> list[str]:
    """Render key findings section lines.

    Returns:
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

    Returns:
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

    Returns:
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
    lines = [f"# cq {result.run.macro}", ""]
    lines.extend(_render_summary(result.summary))
    lines.extend(_render_key_findings(result.key_findings))
    lines.extend(_render_sections(result.sections))
    lines.extend(_render_evidence(result.evidence))
    lines.extend(_render_artifacts(result.artifacts))
    lines.extend(_render_footer(result))
    return "\n".join(lines)


def _get_impact_confidence_summary(findings: list[Finding]) -> tuple[str, str]:
    """Get overall impact and confidence from findings.

    Returns:
    -------
    tuple[str, str]
        (impact_bucket, confidence_bucket) based on findings.
    """
    impact_buckets = []
    conf_buckets = []
    for f in findings:
        impact_value = f.details.get("impact_bucket", None)
        if impact_value is not None:
            impact_buckets.append(str(impact_value))
        conf_value = f.details.get("confidence_bucket", None)
        if conf_value is not None:
            conf_buckets.append(str(conf_value))

    # Use highest impact and confidence seen
    impact_order = {"high": 3, "med": 2, "low": 1}
    conf_order = {"high": 3, "med": 2, "low": 1}

    max_impact = max(impact_buckets, key=lambda x: impact_order.get(x, 0), default="low")
    max_conf = max(conf_buckets, key=lambda x: conf_order.get(x, 0), default="low")

    return max_impact, max_conf


def render_summary(result: CqResult) -> str:
    """Render CqResult as condensed single-line output for CI integration.

    Parameters
    ----------
    result : CqResult
        Analysis result to render.

    Returns:
    -------
    str
        Condensed summary output.
    """
    lines: list[str] = []
    macro = result.run.macro

    # Collect all findings for summary
    all_findings: list[Finding] = []
    all_findings.extend(result.key_findings)
    for section in result.sections:
        all_findings.extend(section.findings)

    if not all_findings:
        return f"{macro}: no findings"

    # Get overall impact/confidence
    impact, confidence = _get_impact_confidence_summary(all_findings)

    # Build summary line based on macro type
    summary_parts: list[str] = []

    if result.summary:
        # Extract key metrics from summary
        for key in ("total_sites", "call_sites", "total_raises", "total_catches"):
            if key in result.summary:
                value = result.summary[key]
                label = key.replace("_", " ")
                summary_parts.append(f"{value} {label}")

        # For sig-impact, show breakage counts
        if "would_break" in result.summary:
            wb = result.summary["would_break"]
            amb = result.summary.get("ambiguous", 0)
            ok = result.summary.get("ok", 0)
            summary_parts = [f"break:{wb}", f"ambiguous:{amb}", f"ok:{ok}"]

    if not summary_parts:
        # Fallback: count findings
        summary_parts.append(f"{len(all_findings)} findings")

    # Count by severity
    severity_counts = {"error": 0, "warning": 0, "info": 0}
    for f in all_findings:
        sev = f.severity if f.severity in severity_counts else "info"
        severity_counts[sev] += 1

    severity_str = ", ".join(
        f"{count} {sev}" for sev, count in severity_counts.items() if count > 0
    )

    summary_line = ", ".join(summary_parts)
    lines.append(
        f"{macro}: {summary_line} [{severity_str}] [impact:{impact} confidence:{confidence}]"
    )

    return "\n".join(lines)
