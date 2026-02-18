"""Summary and insight rendering for CQ results."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from tools.cq.core.contract_codec import encode_json
from tools.cq.core.front_door_render import render_insight_card
from tools.cq.core.front_door_schema import FrontDoorInsightV1
from tools.cq.core.render_utils import na as _na
from tools.cq.core.render_utils import summary_value
from tools.cq.core.serialization import to_builtins
from tools.cq.core.summary_contract import SummaryEnvelopeV1
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding


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


def _summary_int(summary: SummaryEnvelopeV1, key: str) -> int:
    raw = summary_value(summary, key)
    return raw if isinstance(raw, int) else 0


def _derive_query_fallback(result: CqResult) -> str | None:
    run = result.run
    if run.macro == "run":
        steps = result.summary.steps
        if isinstance(steps, (list, tuple)):
            return f"multi-step plan ({len(steps)} steps)"
        step_summaries = result.summary.step_summaries
        if isinstance(step_summaries, Mapping):
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
    value = summary_value(result.summary, key)
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


def _ordered_summary_payload(summary: SummaryEnvelopeV1 | dict[str, object]) -> dict[str, object]:
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
    if isinstance(summary, SummaryEnvelopeV1):
        rendered_summary.setdefault("summary_variant", summary.summary_variant)
    ordered: dict[str, object] = {}
    for key in SUMMARY_PRIORITY_KEYS:
        if key in rendered_summary:
            ordered[key] = rendered_summary[key]
    for key, value in rendered_summary.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def render_summary(summary: SummaryEnvelopeV1 | dict[str, object]) -> list[str]:
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


def render_insight_card_from_summary(summary: SummaryEnvelopeV1 | dict[str, object]) -> list[str]:
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
    raw = summary_value(summary, "front_door_insight")
    if raw is None:
        return []
    if isinstance(raw, FrontDoorInsightV1):
        return render_insight_card(raw)
    if isinstance(raw, Mapping):
        try:
            insight = convert_lax(dict(raw), type_=FrontDoorInsightV1)
            return render_insight_card(insight)
        except BoundaryDecodeError:
            return []
    return []


def _derive_status_from_summary(key: str, value: object) -> str | None:
    """Derive compact diagnostic status lines for artifact-only summary keys.

    Returns:
        str | None: Human-readable status line when derivable, else ``None``.
    """
    if key == "enrichment_telemetry":
        applied = 0
        total = 0
        degraded = 0
        if isinstance(value, Mapping):
            for lang_data in value.values():
                if not isinstance(lang_data, Mapping):
                    continue
                for stage_data in lang_data.values():
                    if not isinstance(stage_data, Mapping):
                        continue
                    applied += int(stage_data.get("applied", 0) or 0)
                    total += int(stage_data.get("total", 0) or 0)
                    degraded += int(stage_data.get("degraded", 0) or 0)
        if total == 0:
            return "Enrichment: none"
        result = f"Enrichment: {applied}/{total} applied"
        return f"{result} | degraded: {degraded}" if degraded else result
    if key in {"python_semantic_telemetry", "rust_semantic_telemetry"}:
        label = "Python semantic" if key == "python_semantic_telemetry" else "Rust semantic"
        if not isinstance(value, Mapping):
            return f"{label}: skipped"
        applied = value.get("applied", 0)
        attempted = value.get("attempted", 0)
        if not attempted:
            return f"{label}: skipped"
        return f"{label}: {applied}/{attempted} applied"
    if key == "semantic_planes":
        if not isinstance(value, Mapping) or not value:
            return "Semantic planes: none"
        counts = value.get("counts")
        if not isinstance(counts, Mapping):
            return "Semantic planes: present"
        tokens = int(counts.get("semantic_tokens", 0) or 0)
        locals_count = int(counts.get("locals", 0) or 0)
        diagnostics = int(counts.get("diagnostics", 0) or 0)
        return f"Semantic planes: tokens={tokens}, locals={locals_count}, diagnostics={diagnostics}"
    if key == "python_semantic_diagnostics":
        count = len(value) if isinstance(value, (list, tuple, Mapping)) else 0
        if count == 0:
            return "Python semantic diagnostics: clean"
        return f"Python semantic diagnostics: {count} items"
    if key == "language_capabilities":
        langs: list[str] = [str(name) for name in value] if isinstance(value, Mapping) else []
        return f"Capabilities: {', '.join(langs)}" if langs else "Capabilities: none"
    if key == "cross_language_diagnostics":
        if isinstance(value, (list, tuple)):
            count = len(value)
        elif isinstance(value, Mapping):
            raw = value.get("diagnostics")
            count = len(raw) if isinstance(raw, (list, tuple)) else 0
        else:
            count = 0
        return "Cross-lang: clean" if count == 0 else f"Cross-lang: {count} diagnostics"
    return None


def compact_summary_for_rendering(
    summary: SummaryEnvelopeV1 | dict[str, object],
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
            status = _derive_status_from_summary(key, value)
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
            value = summary_value(result.summary, key)
            if value is not None:
                label = key.replace("_", " ")
                summary_parts.append(f"{value} {label}")

        # For sig-impact, show breakage counts
        if "would_break" in result.summary:
            summary_parts = [
                f"break:{_summary_int(result.summary, 'would_break')}",
                f"ambiguous:{_summary_int(result.summary, 'ambiguous')}",
                f"ok:{_summary_int(result.summary, 'ok')}",
            ]

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
