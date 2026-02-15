"""Markdown report renderer for cq results."""

from __future__ import annotations

import multiprocessing
import os
from collections.abc import Callable
from pathlib import Path
from typing import cast

from tools.cq.core.contract_codec import dumps_json_value
from tools.cq.core.enrichment_facts import (
    additional_language_payload,
    resolve_fact_clusters,
    resolve_fact_context,
    resolve_primary_language_payload,
)
from tools.cq.core.front_door_insight import (
    FrontDoorInsightV1,
    render_insight_card,
)
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import Artifact, CqResult, Finding, Section
from tools.cq.core.serialization import to_builtins
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax
from tools.cq.query.language import QueryLanguage
from tools.cq.search.objects.render import is_applicability_not_applicable

# Maximum evidence items to show before truncating
MAX_EVIDENCE_DISPLAY = 20
MAX_SECTION_FINDINGS = 50
MAX_RENDER_ENRICH_FILES = 9  # original anchor file + next 8 files
MAX_RENDER_ENRICH_WORKERS = 4
SHOW_UNRESOLVED_FACTS_ENV = "CQ_SHOW_UNRESOLVED_FACTS"
SHOW_CONTEXT_SNIPPETS_ENV = "CQ_RENDER_CONTEXT_SNIPPETS"
MAX_CODE_OVERVIEW_ITEMS = 5
MAX_FACT_VALUE_ITEMS = 8
MAX_FACT_MAPPING_SCALAR_PAIRS = 4
RUN_QUERY_ARG_START_INDEX = 2
MAX_OBJECT_OCCURRENCE_LINES = 200
_TOP_SYMBOL_SKIP_CATEGORIES: frozenset[str] = frozenset(
    {"count", "hot_file", "occurrence", "non_code_occurrence"}
)
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
DETAILS_SUPPRESS_KEYS: frozenset[str] = frozenset(
    {
        "context_snippet",
        "context_window",
        "enrichment",
        "python_enrichment",
        "rust_tree_sitter",
    }
)

# Diagnostics keys whose full payloads are offloaded to artifacts.
# Only compact status lines appear in rendered output.
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


class RenderEnrichmentTask(CqStruct, frozen=True):
    """Process-pool task envelope for render-time enrichment."""

    root: str
    file: str
    line: int
    col: int
    language: QueryLanguage
    candidates: tuple[str, ...]


class RenderEnrichmentResult(CqStruct, frozen=True):
    """Process-pool result envelope for render-time enrichment."""

    file: str
    line: int
    col: int
    language: QueryLanguage
    payload: dict[str, object]


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
        _maybe_attach_render_enrichment(
            f,
            root=root,
            cache=enrich_cache,
            allowed_files=allowed_enrichment_files,
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


def _extract_enrichment_payload(finding: Finding) -> dict[str, object] | None:
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


def _extract_compact_details_payload(finding: Finding) -> dict[str, object] | None:
    data = to_builtins(finding.details.data)
    if not isinstance(data, dict):
        return None
    compact = {
        key: value
        for key, value in data.items()
        if key not in DETAILS_SUPPRESS_KEYS and value is not None
    }
    return compact or None


def _na(reason: str) -> str:
    return f"N/A — {reason.replace('_', ' ')}"


def _show_unresolved_facts() -> bool:
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


def _format_enrichment_facts(payload: dict[str, object]) -> list[str]:
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


def _extract_symbol_hint(finding: Finding) -> str | None:
    for key in ("name", "symbol", "match_text", "callee", "text"):
        value = finding.details.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().split("\n", maxsplit=1)[0]
    message = finding.message.strip()
    if not message:
        return None
    if ":" in message:
        candidate = message.rsplit(":", maxsplit=1)[1].strip()
        if candidate:
            return candidate
    if "(" in message:
        return message.split("(", maxsplit=1)[0].strip() or None
    return message


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


def _derive_query_fallback(result: CqResult) -> str | None:
    run = result.run
    if run.macro == "run":
        steps = result.summary.get("steps")
        if isinstance(steps, list):
            return f"multi-step plan ({len(steps)} steps)"
        step_summaries = result.summary.get("step_summaries")
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


def _summary_string(
    result: CqResult,
    *,
    key: str,
    missing_reason: str,
) -> str:
    summary: dict[str, object] = result.summary if isinstance(result.summary, dict) else {}
    value = summary.get(key)
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


def _format_language_scope(summary: dict[str, object]) -> str:
    lang_scope = summary.get("lang_scope")
    if isinstance(lang_scope, str) and lang_scope:
        return f"`{lang_scope}`"
    language_order = summary.get("language_order")
    if isinstance(language_order, list) and language_order:
        return f"`{', '.join(str(item) for item in language_order)}`"
    step_summaries = summary.get("step_summaries")
    if isinstance(step_summaries, dict):
        inferred_scope = _scope_from_step_summaries(step_summaries)
        if inferred_scope is not None:
            return f"`{inferred_scope}`"
    return _na("language_scope_missing")


def _scope_from_step_summaries(step_summaries: dict[str, object]) -> str | None:
    scopes: set[str] = set()
    ordered: list[str] = []
    for step_summary in step_summaries.values():
        if not isinstance(step_summary, dict):
            continue
        step_scope = step_summary.get("lang_scope")
        if isinstance(step_scope, str) and step_scope:
            scopes.add(step_scope)
        _merge_language_order(ordered, step_summary.get("language_order"))
    if len(scopes) == 1:
        return next(iter(scopes))
    if len(scopes) > 1:
        return "auto"
    if ordered:
        return ", ".join(ordered)
    return None


def _merge_language_order(ordered: list[str], raw_order: object) -> None:
    if not isinstance(raw_order, list):
        return
    for item in raw_order:
        if isinstance(item, str) and item in {"python", "rust"} and item not in ordered:
            ordered.append(item)


def _collect_top_symbols(findings: list[Finding]) -> str:
    symbol_hints: list[str] = []
    seen_symbols: set[str] = set()
    for finding in findings:
        if finding.category in _TOP_SYMBOL_SKIP_CATEGORIES:
            continue
        if finding.anchor is None:
            continue
        symbol = _extract_symbol_hint(finding)
        if symbol is None or symbol in seen_symbols:
            continue
        seen_symbols.add(symbol)
        symbol_hints.append(symbol)
        if len(symbol_hints) >= MAX_CODE_OVERVIEW_ITEMS:
            break
    if not symbol_hints:
        return _na("no_symbol_like_matches")
    return f"`{', '.join(symbol_hints)}`"


def _collect_top_files(findings: list[Finding]) -> str:
    files: list[str] = []
    seen_files: set[str] = set()
    for finding in findings:
        anchor = finding.anchor
        if anchor is None or anchor.file in seen_files:
            continue
        seen_files.add(anchor.file)
        files.append(anchor.file)
        if len(files) >= MAX_CODE_OVERVIEW_ITEMS:
            break
    if not files:
        return _na("no_anchored_matches")
    return f"`{', '.join(files)}`"


def _summarize_categories(findings: list[Finding]) -> str:
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding.category] = counts.get(finding.category, 0) + 1
    if not counts:
        return _na("no_findings")
    ordered_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    category_summary = ", ".join(
        f"{category}:{count}" for category, count in ordered_counts[:MAX_CODE_OVERVIEW_ITEMS]
    )
    return f"`{category_summary}`"


def _format_python_semantic_overview(summary: dict[str, object]) -> str:
    payload = summary.get("python_semantic_overview")
    if not isinstance(payload, dict) or not payload:
        return _na("python_semantic_overview_missing")
    fields: tuple[tuple[str, str], ...] = (
        ("primary_symbol", "primary"),
        ("matches_enriched", "enriched"),
        ("total_incoming_callers", "incoming"),
        ("total_outgoing_callees", "outgoing"),
    )
    parts: list[str] = []
    for key, label in fields:
        value = _clean_scalar(payload.get(key))
        if value is None:
            continue
        parts.append(f"{label}: {value}")
    if not parts:
        return _na("python_semantic_overview_missing")
    return "; ".join(parts)


def _render_code_overview(result: CqResult) -> list[str]:
    lines = ["## Code Overview"]
    all_findings = _iter_result_findings(result)
    lines.append(f"- Query: {_summary_string(result, key='query', missing_reason='query_missing')}")
    lines.append(f"- Mode: {_summary_string(result, key='mode', missing_reason='mode_missing')}")
    summary = result.summary if isinstance(result.summary, dict) else {}
    lines.append(f"- Language Scope: {_format_language_scope(summary)}")
    lines.append(f"- Top Symbols: {_collect_top_symbols(all_findings)}")
    lines.append(f"- Top Files: {_collect_top_files(all_findings)}")
    lines.append(f"- Match Categories: {_summarize_categories(all_findings)}")
    lines.append(f"- Python semantic overview: {_format_python_semantic_overview(summary)}")

    lines.append("")
    return lines


def _infer_language(finding: Finding) -> QueryLanguage:
    language = finding.details.get("language")
    if language in {"python", "rust"}:
        return cast("QueryLanguage", language)
    if finding.anchor and finding.anchor.file.endswith(".rs"):
        return "rust"
    return "python"


def _extract_match_text_candidates(finding: Finding) -> list[str]:
    candidates: list[str] = []
    for key in ("match_text", "name", "callee", "text"):
        value = finding.details.get(key)
        if not isinstance(value, str):
            continue
        candidate = value.strip().split("\n", maxsplit=1)[0]
        if candidate:
            candidates.append(candidate)
    message = finding.message.strip()
    if ":" in message:
        message_candidate = message.rsplit(":", maxsplit=1)[1].strip()
        if message_candidate:
            candidates.append(message_candidate)
    else:
        candidates.append(message)
    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _compute_match_columns(
    line_text: str,
    anchor_col: int,
    candidates: list[str],
) -> tuple[int, int, str]:
    line_len = len(line_text)
    start = max(0, min(anchor_col, line_len))
    for candidate in candidates:
        candidate_start = line_text.find(candidate, start)
        if candidate_start < 0:
            candidate_start = line_text.find(candidate)
        if candidate_start >= 0:
            candidate_end = min(line_len, candidate_start + len(candidate))
            return candidate_start, max(candidate_start + 1, candidate_end), candidate
    if line_len == 0:
        return 0, 1, ""
    end = min(line_len, start + 1)
    return start, max(start + 1, end), line_text[start:end]


def _line_relative_byte_offset(text: str, col: int) -> int:
    safe_col = max(0, min(col, len(text)))
    return len(text[:safe_col].encode("utf-8", errors="replace"))


def _read_line_text(root: Path, rel_path: str, line_number: int) -> str | None:
    if line_number < 1:
        return None
    path = Path(rel_path)
    file_path = path if path.is_absolute() else (root / path)
    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return None
    index = line_number - 1
    if index >= len(lines):
        return None
    return lines[index]


def _merge_enrichment_details(finding: Finding, payload: dict[str, object]) -> None:
    for key, value in payload.items():
        if key in finding.details:
            continue
        finding.details[key] = value


def _compute_render_enrichment_payload_from_anchor(
    *,
    root: Path,
    file: str,
    line: int,
    col: int,
    language: QueryLanguage,
    candidates: list[str],
) -> dict[str, object]:
    line_text = _read_line_text(root, file, line)
    if line_text is None:
        return {}

    from tools.cq.core.locations import SourceSpan
    from tools.cq.search.pipeline.smart_search import RawMatch, build_finding, classify_match

    match_start, match_end, match_text = _compute_match_columns(
        line_text,
        col,
        candidates,
    )
    byte_start = _line_relative_byte_offset(line_text, match_start)
    byte_end = max(byte_start + 1, _line_relative_byte_offset(line_text, match_end))
    raw = RawMatch(
        span=SourceSpan(
            file=file,
            start_line=line,
            start_col=match_start,
            end_line=line,
            end_col=match_end,
        ),
        text=line_text,
        match_text=match_text,
        match_start=match_start,
        match_end=match_end,
        match_byte_start=byte_start,
        match_byte_end=byte_end,
    )
    try:
        enriched = classify_match(
            raw,
            root,
            lang=language,
            force_semantic_enrichment=True,
            enable_python_semantic=True,
        )
        enriched_finding = build_finding(enriched, root)
        payload = to_builtins(enriched_finding.details.data)
    except (OSError, RuntimeError, TypeError, ValueError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _iter_result_findings(result: CqResult) -> list[Finding]:
    findings: list[Finding] = []
    findings.extend(result.key_findings)
    for section in result.sections:
        findings.extend(section.findings)
    findings.extend(result.evidence)
    return findings


def _finding_priority_key(finding: Finding) -> tuple[float, float, str, int, int, str]:
    score = finding.details.get("score")
    numeric_score = float(score) if isinstance(score, (int, float)) else 0.0
    category_weight = {
        "definition": 6.0,
        "callsite": 5.0,
        "import": 4.5,
        "from_import": 4.5,
        "reference": 3.5,
        "assignment": 3.0,
        "annotation": 2.5,
        "comment_match": 1.0,
        "string_match": 1.0,
        "docstring_match": 1.0,
    }.get(finding.category, 2.0)
    anchor = finding.anchor
    if anchor is None:
        return (-numeric_score, -category_weight, "~", 0, 0, finding.message)
    return (
        -numeric_score,
        -category_weight,
        anchor.file,
        anchor.line,
        int(anchor.col or 0),
        finding.message,
    )


def _select_enrichment_target_files(result: CqResult) -> set[str]:
    ranked_findings = sorted(_iter_result_findings(result), key=_finding_priority_key)
    files: list[str] = []
    seen: set[str] = set()
    for finding in ranked_findings:
        anchor = finding.anchor
        if anchor is None or anchor.file in seen:
            continue
        seen.add(anchor.file)
        files.append(anchor.file)
        if len(files) >= MAX_RENDER_ENRICH_FILES:
            break
    return set(files)


def _resolve_render_worker_count(task_count: int) -> int:
    if task_count <= 1:
        return 1
    return min(task_count, MAX_RENDER_ENRICH_WORKERS)


def _compute_render_enrichment_worker(
    task: RenderEnrichmentTask,
) -> RenderEnrichmentResult:
    payload = _compute_render_enrichment_payload_from_anchor(
        root=Path(task.root),
        file=task.file,
        line=task.line,
        col=task.col,
        language=task.language,
        candidates=list(task.candidates),
    )
    return RenderEnrichmentResult(
        file=task.file,
        line=task.line,
        col=task.col,
        language=task.language,
        payload=payload,
    )


def _build_render_enrichment_tasks(
    result: CqResult,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    allowed_files: set[str] | None,
) -> list[RenderEnrichmentTask]:
    tasks: list[RenderEnrichmentTask] = []
    seen: set[tuple[str, int, int, str]] = set()
    for finding in _iter_result_findings(result):
        task = _build_single_render_enrichment_task(
            finding=finding,
            root=root,
            cache=cache,
            seen=seen,
            allowed_files=allowed_files,
        )
        if task is not None:
            tasks.append(task)
    return tasks


def _build_single_render_enrichment_task(
    *,
    finding: Finding,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    seen: set[tuple[str, int, int, str]],
    allowed_files: set[str] | None,
) -> RenderEnrichmentTask | None:
    anchor = finding.anchor
    if anchor is None:
        return None
    if allowed_files is not None and anchor.file not in allowed_files:
        return None
    col = int(anchor.col or 0)
    language = _infer_language(finding)
    cache_key = (anchor.file, anchor.line, col, language)
    if cache_key in cache or cache_key in seen:
        return None
    needs_context = not isinstance(finding.details.get("context_snippet"), str)
    needs_enrichment = _extract_enrichment_payload(finding) is None
    if not (needs_context or needs_enrichment):
        return None
    seen.add(cache_key)
    return RenderEnrichmentTask(
        root=str(root),
        file=anchor.file,
        line=anchor.line,
        col=col,
        language=language,
        candidates=tuple(_extract_match_text_candidates(finding)),
    )


def _populate_render_enrichment_cache(
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    tasks: list[RenderEnrichmentTask],
) -> None:
    workers = _resolve_render_worker_count(len(tasks))
    if workers <= 1:
        _populate_render_enrichment_cache_sequential(cache, tasks)
        return
    scheduler = get_worker_scheduler()
    try:
        futures = [scheduler.submit_cpu(_compute_render_enrichment_worker, task) for task in tasks]
        batch = scheduler.collect_bounded(
            futures,
            timeout_seconds=max(1.0, float(len(tasks))),
        )
        if batch.timed_out > 0:
            _populate_render_enrichment_cache_sequential(cache, tasks)
            return
        for result in batch.done:
            cache[result.file, result.line, result.col, result.language] = result.payload
    except (
        multiprocessing.ProcessError,
        OSError,
        RuntimeError,
        TimeoutError,
        ValueError,
        TypeError,
    ):
        _populate_render_enrichment_cache_sequential(cache, tasks)


def _populate_render_enrichment_cache_sequential(
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    tasks: list[RenderEnrichmentTask],
) -> None:
    for task in tasks:
        result = _compute_render_enrichment_worker(task)
        cache[result.file, result.line, result.col, result.language] = result.payload


def _precompute_render_enrichment_cache(
    result: CqResult,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    allowed_files: set[str] | None,
) -> list[RenderEnrichmentTask]:
    tasks = _build_render_enrichment_tasks(
        result,
        root=root,
        cache=cache,
        allowed_files=allowed_files,
    )
    if not tasks:
        return []
    _populate_render_enrichment_cache(cache, tasks)
    return tasks


def _count_render_enrichment_tasks(
    result: CqResult,
    *,
    root: Path,
    allowed_files: set[str] | None,
) -> int:
    return len(
        _build_render_enrichment_tasks(
            result,
            root=root,
            cache={},
            allowed_files=allowed_files,
        )
    )


def _summary_with_render_enrichment_metrics(
    summary: dict[str, object],
    *,
    attempted: int,
    applied: int,
    failed: int,
    skipped: int,
) -> dict[str, object]:
    with_metrics = dict(summary)
    with_metrics["render_enrichment_attempted"] = attempted
    with_metrics["render_enrichment_applied"] = applied
    with_metrics["render_enrichment_failed"] = failed
    with_metrics["render_enrichment_skipped"] = skipped
    return with_metrics


def _compute_render_enrichment_payload(
    finding: Finding,
    *,
    root: Path,
) -> dict[str, object]:
    anchor = finding.anchor
    if anchor is None:
        return {}
    anchor_col = anchor.col if isinstance(anchor.col, int) else 0
    return _compute_render_enrichment_payload_from_anchor(
        root=root,
        file=anchor.file,
        line=anchor.line,
        col=anchor_col,
        language=_infer_language(finding),
        candidates=_extract_match_text_candidates(finding),
    )


def _maybe_attach_render_enrichment(
    finding: Finding,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]] | None,
    allowed_files: set[str] | None,
) -> None:
    if finding.anchor is None:
        return
    if allowed_files is not None and finding.anchor.file not in allowed_files:
        return
    needs_context = not isinstance(finding.details.get("context_snippet"), str)
    needs_enrichment = _extract_enrichment_payload(finding) is None
    if not (needs_context or needs_enrichment):
        return
    anchor = finding.anchor
    cache_key = (
        anchor.file,
        anchor.line,
        int(anchor.col or 0),
        _infer_language(finding),
    )
    payload: dict[str, object] | None = None
    if cache is not None:
        payload = cache.get(cache_key)
    if payload is None:
        payload = _compute_render_enrichment_payload(finding, root=root)
        if cache is not None:
            cache[cache_key] = payload
    _merge_enrichment_details(finding, payload)


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
    summary_payload = _ordered_summary_payload(summary)
    rendered = dumps_json_value(summary_payload, indent=None)
    lines.append(f"- {rendered}")
    lines.append("")
    return lines


def _ordered_summary_payload(summary: dict[str, object]) -> dict[str, object]:
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


def _render_insight_card_from_summary(summary: dict[str, object]) -> list[str]:
    """Extract and render insight card from summary, if present.

    Returns:
    -------
    list[str]
        Insight card markdown lines, or empty list if not present.
    """
    raw = summary.get("front_door_insight")
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


def compact_summary_for_rendering(
    summary: dict[str, object],
) -> tuple[dict[str, object], list[tuple[str, object]]]:
    """Split summary into compact display and artifact detail payloads.

    Keys in ``ARTIFACT_ONLY_KEYS`` are replaced by compact status lines
    in the returned display dict.  Full payloads are returned separately
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
    for key, value in summary.items():
        if key in ARTIFACT_ONLY_KEYS:
            offloaded.append((key, value))
            status = _derive_compact_status(key, value)
            if status is not None:
                compact[key] = status
        else:
            compact[key] = value
    return compact, offloaded


def _derive_compact_status(key: str, value: object) -> str | None:
    """Derive a compact one-line status from a diagnostic payload.

    Returns:
    -------
    str | None
        Compact status string or None if no deriver available.
    """
    deriver = _COMPACT_STATUS_DERIVERS.get(key)
    if deriver is not None:
        return deriver(value)
    return None


def _derive_enrichment_status(value: object) -> str:
    """Derive compact enrichment telemetry status.

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
    allowed_enrichment_files = _select_enrichment_target_files(result)
    all_task_count = _count_render_enrichment_tasks(result, root=root, allowed_files=None)
    rendered_tasks = _precompute_render_enrichment_cache(
        result,
        root=root,
        cache=enrich_cache,
        allowed_files=allowed_enrichment_files,
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
