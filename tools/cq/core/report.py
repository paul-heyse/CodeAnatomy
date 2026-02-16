"""Markdown report renderer for cq results."""

from __future__ import annotations

import multiprocessing
import os
from pathlib import Path
from typing import cast

from tools.cq.core.render_diagnostics import (
    summary_with_render_enrichment_metrics as _summary_with_render_enrichment_metrics,
)
from tools.cq.core.render_enrichment import (
    extract_enrichment_payload as _extract_enrichment_payload,
)
from tools.cq.core.render_enrichment import (
    format_enrichment_facts as _format_enrichment_facts,
)
from tools.cq.core.render_enrichment import (
    merge_enrichment_details as _merge_enrichment_details,
)
from tools.cq.core.render_overview import render_code_overview as _render_code_overview
from tools.cq.core.render_summary import (
    compact_summary_for_rendering,
    render_insight_card_from_summary as _render_insight_card_from_summary,
    render_summary as _render_summary,
    render_summary_condensed,
)
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import Artifact, CqResult, Finding, Section
from tools.cq.core.structs import CqStruct
from tools.cq.core.type_coercion import coerce_float
from tools.cq.query.language import QueryLanguage

# Maximum evidence items to show before truncating
MAX_EVIDENCE_DISPLAY = 20
MAX_SECTION_FINDINGS = 50
MAX_RENDER_ENRICH_FILES = 9  # original anchor file + next 8 files
MAX_RENDER_ENRICH_WORKERS = 4
SHOW_CONTEXT_SNIPPETS_ENV = "CQ_RENDER_CONTEXT_SNIPPETS"
MAX_OBJECT_OCCURRENCE_LINES = 200

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
    from tools.cq.search.pipeline.smart_search import build_finding, classify_match
    from tools.cq.search.pipeline.smart_search_types import RawMatch

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
    try:
        numeric_score = coerce_float(score)
    except TypeError:
        numeric_score = 0.0
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


# Public API re-exports
render_summary = render_summary_condensed

from tools.cq.core.render_summary import ARTIFACT_ONLY_KEYS

__all__ = [
    "ARTIFACT_ONLY_KEYS",
    "compact_summary_for_rendering",
    "render_markdown",
    "render_summary",
]
