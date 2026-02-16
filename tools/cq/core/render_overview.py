"""Code overview rendering for CQ results."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding


MAX_CODE_OVERVIEW_ITEMS = 5
_TOP_SYMBOL_SKIP_CATEGORIES: frozenset[str] = frozenset(
    {"count", "hot_file", "occurrence", "non_code_occurrence"}
)


def _na(reason: str) -> str:
    return f"N/A — {reason.replace('_', ' ')}"


def _clean_scalar(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return str(value)
    return None


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


def _collect_top_symbols(findings: list[Finding]) -> str:
    """Collect top symbols from findings.

    Parameters
    ----------
    findings : list[Finding]
        Findings to extract symbols from.

    Returns
    -------
    str
        Formatted symbol list or N/A message.
    """
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
    """Collect top files from findings.

    Parameters
    ----------
    findings : list[Finding]
        Findings to extract files from.

    Returns
    -------
    str
        Formatted file list or N/A message.
    """
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
    """Summarize finding categories with counts.

    Parameters
    ----------
    findings : list[Finding]
        Findings to summarize.

    Returns
    -------
    str
        Formatted category summary or N/A message.
    """
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
    """Format Python semantic overview from summary.

    Parameters
    ----------
    summary : dict[str, object]
        Result summary dict.

    Returns
    -------
    str
        Formatted overview or N/A message.
    """
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


def _merge_language_order(ordered: list[str], raw_order: object) -> None:
    """Merge language order from step summaries.

    Parameters
    ----------
    ordered : list[str]
        Accumulator for ordered languages.
    raw_order : object
        Raw language order from step summary.
    """
    if not isinstance(raw_order, list):
        return
    for item in raw_order:
        if isinstance(item, str) and item in {"python", "rust"} and item not in ordered:
            ordered.append(item)


def _scope_from_step_summaries(step_summaries: dict[str, object]) -> str | None:
    """Infer language scope from step summaries.

    Parameters
    ----------
    step_summaries : dict[str, object]
        Step summaries dict.

    Returns
    -------
    str | None
        Inferred scope or None.
    """
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


def _format_language_scope(summary: dict[str, object]) -> str:
    """Format language scope from summary.

    Parameters
    ----------
    summary : dict[str, object]
        Result summary dict.

    Returns
    -------
    str
        Formatted language scope or N/A message.
    """
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


def _iter_result_findings(result: CqResult) -> list[Finding]:
    """Collect all findings from result.

    Parameters
    ----------
    result : CqResult
        Analysis result.

    Returns
    -------
    list[Finding]
        All findings.
    """
    findings: list[Finding] = []
    findings.extend(result.key_findings)
    for section in result.sections:
        findings.extend(section.findings)
    findings.extend(result.evidence)
    return findings


def render_code_overview(result: CqResult) -> list[str]:
    """Render code overview section.

    Parameters
    ----------
    result : CqResult
        Analysis result to render.

    Returns
    -------
    list[str]
        Overview section lines.
    """
    from tools.cq.core.render_summary import summary_string

    lines = ["## Code Overview"]
    all_findings = _iter_result_findings(result)
    lines.append(f"- Query: {summary_string(result, key='query', missing_reason='query_missing')}")
    lines.append(f"- Mode: {summary_string(result, key='mode', missing_reason='mode_missing')}")
    summary = result.summary if isinstance(result.summary, dict) else {}
    lines.append(f"- Language Scope: {_format_language_scope(summary)}")
    lines.append(f"- Top Symbols: {_collect_top_symbols(all_findings)}")
    lines.append(f"- Top Files: {_collect_top_files(all_findings)}")
    lines.append(f"- Match Categories: {_summarize_categories(all_findings)}")
    lines.append(f"- Python semantic overview: {_format_python_semantic_overview(summary)}")

    lines.append("")
    return lines


__all__ = ["render_code_overview"]
