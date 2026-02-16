"""Summary and insight rendering for CQ results."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.core.contract_codec import encode_json
from tools.cq.core.front_door_render import render_insight_card
from tools.cq.core.front_door_schema import FrontDoorInsightV1
from tools.cq.core.render_utils import na as _na
from tools.cq.core.serialization import to_builtins
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding
    from tools.cq.core.summary_contract import CqSummary


SUMMARY_PRIORITY_KEYS: tuple[str, ...] = (
    "query",
    "mode",
    "lang_scope",
    "language_order",
    "returned_matches",
    "total_matches",
    "matched_files",
    "scanned_files",
    "caps_hit",
    "truncated",
    "timed_out",
    "languages",
    "cross_language_diagnostics",
    "language_capabilities",
    "enrichment_telemetry",
    "python_semantic_overview",
    "python_semantic_telemetry",
    "rust_semantic_telemetry",
    "semantic_planes",
    "python_semantic_diagnostics",
)

ARTIFACT_ONLY_KEYS: frozenset[str] = frozenset(
    {
        "enrichment_telemetry",
        "python_semantic_telemetry",
        "rust_semantic_telemetry",
        "semantic_planes",
        "python_semantic_diagnostics",
        "language_capabilities",
        "cross_language_diagnostics",
    }
)

RUN_QUERY_ARG_START_INDEX = 2


def _summary_value(summary: CqSummary | dict[str, object], key: str) -> object:
    if isinstance(summary, dict):
        return summary.get(key)
    return getattr(summary, key, None)


def _derive_query_fallback(result: CqResult) -> str | None:
    run = result.run
    if run.macro == "run":
        steps = result.summary.steps
        if isinstance(steps, list):
            return f"multi-step plan ({len(steps)} steps)"
        step_summaries = result.summary.step_summaries
        if isinstance(step_summaries, dict):
            return f"multi-step plan ({len(step_summaries)} steps)"
        return "multi-step plan"
    if len(run.argv) > RUN_QUERY_ARG_START_INDEX:
        tail = " ".join(
            str(arg) for arg in run.argv[RUN_QUERY_ARG_START_INDEX:] if str(arg).strip()
        )
        if tail:
            return tail
    return None


def _derive_mode_fallback(result: CqResult) -> str | None:
    run_macro = result.run.macro
    if run_macro == "run":
        return "run"
    if run_macro in {
        "calls",
        "impact",
        "sig-impact",
        "imports",
        "exceptions",
        "side-effects",
        "scopes",
        "bytecode-surface",
    }:
        return f"macro:{run_macro}"
    if run_macro in {"search", "q"}:
        return run_macro
    return None


def summary_string(
    result: CqResult,
    *,
    key: str,
    missing_reason: str,
) -> str:
    """Extract summary string with fallback logic.

    Parameters
    ----------
    result : CqResult
        Analysis result.
    key : str
        Summary key to extract.
    missing_reason : str
        Reason code for N/A message.

    Returns:
    -------
    str
        Formatted summary value or N/A message.
    """
    value = _summary_value(result.summary, key)
    if isinstance(value, str) and value:
        return f"`{value}`"
    fallback: str | None = None
    if key == "query":
        fallback = _derive_query_fallback(result)
    elif key == "mode":
        fallback = _derive_mode_fallback(result)
    if isinstance(fallback, str) and fallback:
        return f"`{fallback}`"
    return _na(missing_reason)


def _ordered_summary_payload(summary: CqSummary | dict[str, object]) -> dict[str, object]:
    """Order summary keys by priority.

    Parameters
    ----------
    summary : dict[str, object]
        Summary dict.

    Returns:
    -------
    dict[str, object]
        Ordered summary dict.
    """
    rendered_summary = to_builtins(summary)
    if not isinstance(rendered_summary, dict):
        return {"summary": rendered_summary}
    ordered: dict[str, object] = {}
    for key in SUMMARY_PRIORITY_KEYS:
        if key in rendered_summary:
            ordered[key] = rendered_summary[key]
    for key, value in rendered_summary.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def render_summary(summary: CqSummary | dict[str, object]) -> list[str]:
    """Render summary section lines.

    Parameters
    ----------
    summary : dict[str, object]
        Summary dict.

    Returns:
    -------
    list[str]
        Summary section lines.
    """
    if not summary:
        return []
    lines = ["## Summary"]
    summary_payload = _ordered_summary_payload(summary)
    rendered = encode_json(summary_payload, indent=None)
    lines.append(f"- {rendered}")
    lines.append("")
    return lines


def render_insight_card_from_summary(summary: CqSummary | dict[str, object]) -> list[str]:
    """Extract and render insight card from summary.

    Parameters
    ----------
    summary : dict[str, object]
        Summary dict.

    Returns:
    -------
    list[str]
        Insight card markdown lines, or empty list if not present.
    """
    raw = _summary_value(summary, "front_door_insight")
    if raw is None:
        return []
    if isinstance(raw, FrontDoorInsightV1):
        return render_insight_card(raw)
    if isinstance(raw, dict):
        try:
            insight = convert_lax(raw, type_=FrontDoorInsightV1)
            return render_insight_card(insight)
        except BoundaryDecodeError:
            return []
    return []


def _derive_enrichment_status(value: object) -> str:
    """Derive compact enrichment telemetry status.

    Parameters
    ----------
    value : object
        Enrichment telemetry payload.

    Returns:
    -------
    str
        Compact status line.
    """
    applied = 0
    total = 0
    degraded = 0
    if isinstance(value, dict):
        for lang_data in value.values():
            if isinstance(lang_data, dict):
                for stage_data in lang_data.values():
                    if isinstance(stage_data, dict):
                        applied += int(stage_data.get("applied", 0) or 0)
                        total += int(stage_data.get("total", 0) or 0)
                        degraded += int(stage_data.get("degraded", 0) or 0)
    if total == 0:
        return "Enrichment: none"
    result = f"Enrichment: {applied}/{total} applied"
    if degraded:
        result += f" | degraded: {degraded}"
    return result


def _derive_python_semantic_telemetry_status(value: object) -> str:
    """Derive compact Python semantic telemetry status.

    Parameters
    ----------
    value : object
        Python semantic telemetry payload.

    Returns:
    -------
    str
        Compact status line.
    """
    if not isinstance(value, dict):
        return "Python semantic: skipped"
    applied = value.get("applied", 0)
    attempted = value.get("attempted", 0)
    if not attempted:
        return "Python semantic: skipped"
    return f"Python semantic: {applied}/{attempted} applied"


def _derive_rust_semantic_telemetry_status(value: object) -> str:
    """Derive compact rust semantic telemetry status.

    Parameters
    ----------
    value : object
        Rust semantic telemetry payload.

    Returns:
    -------
    str
        Compact status line.
    """
    if not isinstance(value, dict):
        return "Rust semantic: skipped"
    applied = value.get("applied", 0)
    attempted = value.get("attempted", 0)
    if not attempted:
        return "Rust semantic: skipped"
    return f"Rust semantic: {applied}/{attempted} applied"


def _derive_semantic_advanced_status(value: object) -> str:
    """Derive compact status for semantic planes.

    Parameters
    ----------
    value : object
        Semantic planes payload.

    Returns:
    -------
    str
        Compact status line.
    """
    if not isinstance(value, dict) or not value:
        return "Semantic planes: none"
    counts = value.get("counts")
    if not isinstance(counts, dict):
        return "Semantic planes: present"
    tokens = int(counts.get("semantic_tokens", 0) or 0)
    locals_count = int(counts.get("locals", 0) or 0)
    diagnostics = int(counts.get("diagnostics", 0) or 0)
    return f"Semantic planes: tokens={tokens}, locals={locals_count}, diagnostics={diagnostics}"


def _derive_python_semantic_diagnostics_status(value: object) -> str:
    """Derive compact Python semantic diagnostics status.

    Parameters
    ----------
    value : object
        Python semantic diagnostics payload.

    Returns:
    -------
    str
        Compact status line.
    """
    count = len(value) if isinstance(value, (list, dict)) else 0
    if count == 0:
        return "Python semantic diagnostics: clean"
    return f"Python semantic diagnostics: {count} items"


def _derive_capabilities_status(value: object) -> str:
    """Derive compact language capabilities status.

    Parameters
    ----------
    value : object
        Language capabilities payload.

    Returns:
    -------
    str
        Compact status line.
    """
    langs: list[str] = []
    if isinstance(value, dict):
        langs = [str(key) for key in value]
    return f"Capabilities: {', '.join(langs)}" if langs else "Capabilities: none"


def _derive_cross_lang_status(value: object) -> str:
    """Derive compact cross-language diagnostics status.

    Parameters
    ----------
    value : object
        Cross-language diagnostics payload.

    Returns:
    -------
    str
        Compact status line.
    """
    count = 0
    if isinstance(value, list):
        count = len(value)
    elif isinstance(value, dict):
        raw = value.get("diagnostics")
        count = len(raw) if isinstance(raw, list) else 0
    if count == 0:
        return "Cross-lang: clean"
    return f"Cross-lang: {count} diagnostics"


_CompactDeriver = Callable[[object], str]

_COMPACT_STATUS_DERIVERS: dict[str, _CompactDeriver] = {
    "enrichment_telemetry": _derive_enrichment_status,
    "python_semantic_telemetry": _derive_python_semantic_telemetry_status,
    "rust_semantic_telemetry": _derive_rust_semantic_telemetry_status,
    "semantic_planes": _derive_semantic_advanced_status,
    "python_semantic_diagnostics": _derive_python_semantic_diagnostics_status,
    "language_capabilities": _derive_capabilities_status,
    "cross_language_diagnostics": _derive_cross_lang_status,
}


def _derive_compact_status(key: str, value: object) -> str | None:
    """Derive a compact one-line status from a diagnostic payload.

    Parameters
    ----------
    key : str
        Summary key.
    value : object
        Diagnostic payload.

    Returns:
    -------
    str | None
        Compact status string or None if no deriver available.
    """
    deriver = _COMPACT_STATUS_DERIVERS.get(key)
    if deriver is not None:
        return deriver(value)
    return None


def compact_summary_for_rendering(
    summary: CqSummary | dict[str, object],
) -> tuple[dict[str, object], list[tuple[str, object]]]:
    """Split summary into compact display and artifact detail payloads.

    Keys in ``ARTIFACT_ONLY_KEYS`` are replaced by compact status lines
    in the returned display dict. Full payloads are returned separately
    for offloading into collapsed artifact sections.

    Parameters
    ----------
    summary : dict[str, object]
        Full summary dict.

    Returns:
    -------
    tuple[dict[str, object], list[tuple[str, object]]]
        (compact display dict, offloaded (key, payload) pairs).
    """
    compact: dict[str, object] = {}
    offloaded: list[tuple[str, object]] = []
    items = summary.items() if isinstance(summary, dict) else summary.to_dict().items()
    for key, value in items:
        if key in ARTIFACT_ONLY_KEYS:
            offloaded.append((key, value))
            status = _derive_compact_status(key, value)
            if status is not None:
                compact[key] = status
        else:
            compact[key] = value
    return compact, offloaded


def get_impact_confidence_summary(findings: list[Finding]) -> tuple[str, str]:
    """Get overall impact and confidence from findings.

    Parameters
    ----------
    findings : list[Finding]
        Findings to analyze.

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


def render_summary_condensed(result: CqResult) -> str:
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
    impact, confidence = get_impact_confidence_summary(all_findings)

    # Build summary line based on macro type
    summary_parts: list[str] = []

    if result.summary:
        # Extract key metrics from summary
        for key in ("total_sites", "call_sites", "total_raises", "total_catches"):
            value = _summary_value(result.summary, key)
            if value is not None:
                label = key.replace("_", " ")
                summary_parts.append(f"{value} {label}")

        # For sig-impact, show breakage counts
        if "would_break" in result.summary:
            wb = result.summary.would_break
            amb = result.summary.ambiguous
            ok = result.summary.ok
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


__all__ = [
    "ARTIFACT_ONLY_KEYS",
    "SUMMARY_PRIORITY_KEYS",
    "compact_summary_for_rendering",
    "get_impact_confidence_summary",
    "render_insight_card_from_summary",
    "render_summary",
    "render_summary_condensed",
    "summary_string",
]
