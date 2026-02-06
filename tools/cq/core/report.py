"""Markdown report renderer for cq results."""

from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import cast

from tools.cq.core.codec import dumps_json_value
from tools.cq.core.enrichment_facts import (
    additional_language_payload,
    resolve_fact_clusters,
    resolve_fact_context,
    resolve_primary_language_payload,
)
from tools.cq.core.schema import Artifact, CqResult, Finding, Section
from tools.cq.core.serialization import to_builtins
from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage

# Maximum evidence items to show before truncating
MAX_EVIDENCE_DISPLAY = 20
MAX_SECTION_FINDINGS = 50
MAX_RENDER_ENRICH_FILES = 9  # original anchor file + next 8 files
MAX_RENDER_ENRICH_WORKERS = 4
MAX_CODE_OVERVIEW_ITEMS = 5
RUN_QUERY_ARG_START_INDEX = 2
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

    rendered_lines.extend(_format_context_block(f))

    details_payload = _extract_compact_details_payload(f)
    if details_payload is not None:
        rendered = dumps_json_value(details_payload, indent=None)
        rendered_lines.append(f"  Details: `{rendered}`")

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


def _format_context_block(finding: Finding) -> list[str]:
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


def _format_fact_value(value: object) -> str:
    rendered = (
        value if isinstance(value, str) else dumps_json_value(to_builtins(value), indent=None)
    )
    if not isinstance(rendered, str):
        rendered = str(rendered)
    return rendered.strip() or _na("not_resolved")


def _format_enrichment_facts(payload: dict[str, object]) -> list[str]:
    language, language_payload = resolve_primary_language_payload(payload)
    context = resolve_fact_context(language=language, language_payload=language_payload)
    clusters = resolve_fact_clusters(
        context=context,
        language_payload=language_payload,
    )
    lines = ["  Code Facts:"]
    for cluster in clusters:
        lines.append(f"  - {cluster.title}")
        for row in cluster.rows:
            rendered = _format_fact_value(row.value) if row.reason is None else _na(row.reason)
            lines.append(f"    - {row.label}: {rendered}")

    additional_payload = additional_language_payload(language_payload)
    if additional_payload:
        rendered = dumps_json_value(to_builtins(additional_payload), indent=None)
        lines.append(f"  - Additional Facts: `{rendered}`")
    if isinstance(language, str):
        lines.append(f"  - Enrichment Language: `{language}`")
    return lines


def _extract_symbol_hint(finding: Finding) -> str | None:
    for key in ("name", "match_text", "callee", "text"):
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
        scopes: set[str] = set()
        ordered: list[str] = []
        for step_summary in step_summaries.values():
            if not isinstance(step_summary, dict):
                continue
            step_scope = step_summary.get("lang_scope")
            if isinstance(step_scope, str) and step_scope:
                scopes.add(step_scope)
            step_order = step_summary.get("language_order")
            if isinstance(step_order, list):
                for item in step_order:
                    if isinstance(item, str) and item in {"python", "rust"} and item not in ordered:
                        ordered.append(item)
        if len(scopes) == 1:
            return f"`{next(iter(scopes))}`"
        if len(scopes) > 1:
            return "`auto`"
        if ordered:
            return f"`{', '.join(ordered)}`"
    return _na("language_scope_missing")


def _collect_top_symbols(findings: list[Finding]) -> str:
    symbol_hints: list[str] = []
    seen_symbols: set[str] = set()
    for finding in findings:
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
    from tools.cq.search.smart_search import RawMatch, build_finding, classify_match

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
        )
        enriched_finding = build_finding(enriched, root)
        payload = to_builtins(enriched_finding.details.data)
    except Exception:  # noqa: BLE001 - fail-open render enrichment
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


def _select_enrichment_target_files(result: CqResult) -> set[str]:
    files: list[str] = []
    seen: set[str] = set()
    for finding in _iter_result_findings(result):
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
    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            for result in pool.map(_compute_render_enrichment_worker, tasks):
                cache[result.file, result.line, result.col, result.language] = result.payload
    except Exception:  # noqa: BLE001 - fail-open to sequential mode
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
) -> None:
    tasks = _build_render_enrichment_tasks(
        result,
        root=root,
        cache=cache,
        allowed_files=allowed_files,
    )
    if not tasks:
        return
    _populate_render_enrichment_cache(cache, tasks)


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
                root=root,
                enrich_cache=enrich_cache,
                allowed_enrichment_files=allowed_enrichment_files,
            )
            for finding in findings
        ]
    )
    lines.append("")
    return lines


def _render_sections(
    sections: list[Section],
    *,
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
) -> list[str]:
    """Render section blocks.

    Returns:
    -------
    list[str]
        Rendered section lines.
    """
    lines: list[str] = []
    for section in sections:
        lines.append(
            _format_section(
                section,
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
    root: Path | None = None,
    enrich_cache: dict[tuple[str, int, int, str], dict[str, object]] | None = None,
    allowed_enrichment_files: set[str] | None = None,
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
    displayed = findings[:MAX_EVIDENCE_DISPLAY]
    lines.extend(
        [
            _format_finding(
                finding,
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
    _precompute_render_enrichment_cache(
        result,
        root=root,
        cache=enrich_cache,
        allowed_files=allowed_enrichment_files,
    )

    lines = [f"# cq {result.run.macro}", ""]
    lines.extend(_render_code_overview(result))
    lines.extend(
        _render_key_findings(
            result.key_findings,
            root=root,
            enrich_cache=enrich_cache,
            allowed_enrichment_files=allowed_enrichment_files,
        )
    )
    lines.extend(
        _render_sections(
            result.sections,
            root=root,
            enrich_cache=enrich_cache,
            allowed_enrichment_files=allowed_enrichment_files,
        )
    )
    lines.extend(
        _render_evidence(
            result.evidence,
            root=root,
            enrich_cache=enrich_cache,
            allowed_enrichment_files=allowed_enrichment_files,
        )
    )
    lines.extend(_render_artifacts(result.artifacts))
    lines.extend(_render_summary(result.summary))
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
