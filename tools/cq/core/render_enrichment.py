"""Enrichment rendering logic for CQ results.

This module extracts enrichment-related rendering functions from report.py,
breaking the core→search dependency via callback injection.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from tools.cq.core.enrichment_facts import (
    additional_language_payload,
    resolve_fact_clusters,
    resolve_fact_context,
    resolve_primary_language_payload,
)
from tools.cq.core.schema import Finding
from tools.cq.core.serialization import to_builtins
from tools.cq.search.objects.render import is_applicability_not_applicable

MAX_FACT_VALUE_ITEMS = 8
MAX_FACT_MAPPING_SCALAR_PAIRS = 4
SHOW_UNRESOLVED_FACTS_ENV = "CQ_SHOW_UNRESOLVED_FACTS"

DETAILS_SUPPRESS_KEYS: frozenset[str] = frozenset(
    {
        "context_snippet",
        "context_window",
        "enrichment",
        "python_enrichment",
        "rust_tree_sitter",
    }
)

# Callback type for render-time enrichment computation
EnrichmentCallback = Callable[[Finding, Path], dict[str, object]]


def extract_enrichment_payload(finding: Finding) -> dict[str, object] | None:
    """Extract enrichment payload from finding details.

    Parameters
    ----------
    finding : Finding
        Finding to extract enrichment from.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or None if not present.
    """
    payload = finding.details.get("enrichment")
    if isinstance(payload, dict) and payload:
        return payload
    fallback: dict[str, object] = {}
    python_payload = finding.details.get("python_enrichment")
    if isinstance(python_payload, dict) and python_payload:
        fallback["python"] = python_payload
    rust_payload = finding.details.get("rust_tree_sitter")
    if isinstance(rust_payload, dict) and rust_payload:
        fallback["rust"] = rust_payload
    language = finding.details.get("language")
    if fallback and isinstance(language, str):
        fallback["language"] = language
    return fallback or None


def extract_compact_details_payload(finding: Finding) -> dict[str, object] | None:
    """Extract compact details payload, excluding enrichment keys.

    Parameters
    ----------
    finding : Finding
        Finding to extract details from.

    Returns:
    -------
    dict[str, object] | None
        Compact details payload, or None if empty.
    """
    data = to_builtins(finding.details.data)
    if not isinstance(data, dict):
        return None
    compact = {
        key: value
        for key, value in data.items()
        if key not in DETAILS_SUPPRESS_KEYS and value is not None
    }
    return compact or None


def merge_enrichment_details(finding: Finding, payload: dict[str, object]) -> None:
    """Merge enrichment payload into finding details.

    Parameters
    ----------
    finding : Finding
        Finding to update.
    payload : dict[str, object]
        Enrichment payload to merge.
    """
    for key, value in payload.items():
        if key in finding.details:
            continue
        finding.details[key] = value


def _na(reason: str) -> str:
    return f"N/A — {reason.replace('_', ' ')}"


def _show_unresolved_facts() -> bool:
    import os

    value = os.environ.get(SHOW_UNRESOLVED_FACTS_ENV, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _clean_scalar(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _safe_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _format_location(
    file_value: str | None, line_value: int | None, col_value: int | None
) -> str | None:
    if not file_value and line_value is None:
        return None
    base = file_value or "<unknown>"
    if line_value is not None and line_value > 0:
        base = f"{base}:{line_value}"
        if col_value is not None and col_value >= 0:
            base = f"{base}:{col_value}"
    return base


def _format_target_mapping(value: dict[str, object]) -> str | None:
    symbol = None
    for key in ("symbol", "name", "label"):
        symbol = _clean_scalar(value.get(key))
        if symbol:
            break
    kind = _clean_scalar(value.get("kind"))
    file_value = _clean_scalar(value.get("file")) or _clean_scalar(value.get("resolved_path"))
    line_value = _safe_int(value.get("line"))
    col_value = _safe_int(value.get("col"))
    location = _format_location(file_value, line_value, col_value)

    if symbol and location:
        kind_suffix = f" [{kind}]" if kind and kind not in {"reference"} else ""
        return f"{symbol}{kind_suffix} @ {location}"
    if location:
        kind_prefix = f"{kind}: " if kind else ""
        return f"{kind_prefix}{location}"
    return symbol


def _format_diagnostic_mapping(value: dict[str, object]) -> str | None:
    message = _clean_scalar(value.get("message"))
    if not message:
        return None
    severity = _clean_scalar(value.get("severity"))
    code = _clean_scalar(value.get("code"))
    line_value = _safe_int(value.get("line"))
    col_value = _safe_int(value.get("col"))
    file_value = _clean_scalar(value.get("file"))
    location = _format_location(file_value, line_value, col_value)
    prefix_parts = [part for part in (severity, code) if part]
    prefix = " ".join(prefix_parts)
    if prefix:
        prefix = f"{prefix}: "
    if location:
        return f"{prefix}{message} @ {location}"
    return f"{prefix}{message}"


def _format_name_source_mapping(value: dict[str, object]) -> str | None:
    label = _clean_scalar(value.get("name")) or _clean_scalar(value.get("label"))
    source = _clean_scalar(value.get("source"))
    if label and source:
        return f"{label} ({source})"
    return label


def _format_scalar_pairs_mapping(value: dict[str, object]) -> str | None:
    scalar_pairs: list[str] = []
    for key in sorted(value):
        if key.startswith("byte"):
            continue
        scalar = _clean_scalar(value.get(key))
        if scalar is None:
            continue
        scalar_pairs.append(f"{key}={scalar}")
        if len(scalar_pairs) >= MAX_FACT_MAPPING_SCALAR_PAIRS:
            break
    if scalar_pairs:
        return ", ".join(scalar_pairs)
    return None


def _format_fact_mapping(value: dict[str, object]) -> str | None:
    if "message" in value and ("severity" in value or "code" in value):
        diagnostic_line = _format_diagnostic_mapping(value)
        if diagnostic_line:
            return diagnostic_line

    target_line = _format_target_mapping(value)
    if target_line:
        return target_line

    name_source_line = _format_name_source_mapping(value)
    if name_source_line:
        return name_source_line

    return _format_scalar_pairs_mapping(value)


def _append_formatted_items(items: list[str], value: object) -> None:
    if len(items) >= MAX_FACT_VALUE_ITEMS:
        return
    scalar = _clean_scalar(value)
    if scalar is not None:
        items.append(scalar)
        return
    if isinstance(value, dict):
        line = _format_fact_mapping(value)
        if line:
            items.append(line)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _append_formatted_items(items, item)
            if len(items) >= MAX_FACT_VALUE_ITEMS:
                return


def _format_fact_values(value: object) -> list[str]:
    rendered: list[str] = []
    _append_formatted_items(rendered, value)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in rendered:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def format_enrichment_facts(payload: dict[str, object]) -> list[str]:
    """Format enrichment facts into markdown lines.

    Parameters
    ----------
    payload : dict[str, object]
        Enrichment payload to format.

    Returns:
    -------
    list[str]
        Markdown-formatted fact lines.
    """
    language, language_payload = resolve_primary_language_payload(payload)
    context = resolve_fact_context(language=language, language_payload=language_payload)
    clusters = resolve_fact_clusters(
        context=context,
        language_payload=language_payload,
    )
    lines = ["  Code Facts:"]
    show_unresolved = _show_unresolved_facts()
    for cluster in clusters:
        candidate_rows = [
            row for row in cluster.rows if not is_applicability_not_applicable(row.reason)
        ]
        rows = (
            candidate_rows
            if show_unresolved
            else [row for row in candidate_rows if row.reason != "not_resolved"]
        )
        if not rows:
            continue
        lines.append(f"  - {cluster.title}")
        for row in rows:
            if row.reason is not None:
                if show_unresolved and row.reason == "not_resolved":
                    lines.append(f"    - {row.label}: {_na(row.reason)}")
                continue
            values = _format_fact_values(row.value)
            if not values:
                continue
            for idx, rendered in enumerate(values, start=1):
                suffix = "" if idx == 1 else f" #{idx}"
                lines.append(f"    - {row.label}{suffix}: {rendered}")

    # Suppress raw JSON "Additional Facts" payloads in markdown output.
    _ = additional_language_payload(language_payload)
    if isinstance(language, str):
        lines.append(f"  - Enrichment Language: `{language}`")
    return lines


__all__ = [
    "EnrichmentCallback",
    "extract_compact_details_payload",
    "extract_enrichment_payload",
    "format_enrichment_facts",
    "merge_enrichment_details",
]
