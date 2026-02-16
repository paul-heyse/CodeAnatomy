"""Render-time enrichment task orchestration helpers."""

from __future__ import annotations

import multiprocessing
from pathlib import Path
from typing import TYPE_CHECKING, cast

from tools.cq.core.render_enrichment import extract_enrichment_payload, merge_enrichment_details
from tools.cq.core.render_utils import iter_result_findings
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.schema import CqResult, Finding
from tools.cq.core.structs import CqStruct
from tools.cq.core.type_coercion import coerce_float_optional
from tools.cq.query.language import QueryLanguage

if TYPE_CHECKING:
    from tools.cq.core.ports import RenderEnrichmentPort

MAX_RENDER_ENRICH_FILES = 9
MAX_RENDER_ENRICH_WORKERS = 4


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


def infer_language(finding: Finding) -> QueryLanguage:
    """Infer language for one finding anchor.

    Returns:
    -------
    QueryLanguage
        Inferred language label.
    """
    language = finding.details.get("language")
    if language in {"python", "rust"}:
        return cast("QueryLanguage", language)
    if finding.anchor and finding.anchor.file.endswith(".rs"):
        return "rust"
    return "python"


def extract_match_text_candidates(finding: Finding) -> list[str]:
    """Extract stable candidate text snippets for anchor enrichment.

    Returns:
    -------
    list[str]
        De-duplicated candidate snippets ordered by priority.
    """
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


def finding_priority_key(finding: Finding) -> tuple[float, float, str, int, int, str]:
    """Stable ranking key for selecting enrichment target files.

    Returns:
    -------
    tuple[float, float, str, int, int, str]
        Sort key for descending score/category and deterministic tie-breaks.
    """
    score = finding.details.get("score")
    numeric_score = coerce_float_optional(score) or 0.0
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


def select_enrichment_target_files(result: CqResult) -> set[str]:
    """Pick highest-value files for render-time enrichment work.

    Returns:
    -------
    set[str]
        Top-ranked files selected for enrichment.
    """
    ranked_findings = sorted(iter_result_findings(result), key=finding_priority_key)
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


def _compute_payload_from_task(
    *,
    task: RenderEnrichmentTask,
    port: RenderEnrichmentPort | None,
) -> RenderEnrichmentResult:
    if port is None:
        payload: dict[str, object] = {}
    else:
        payload = port.enrich_anchor(
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


def build_render_enrichment_tasks(
    result: CqResult,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    allowed_files: set[str] | None,
) -> list[RenderEnrichmentTask]:
    """Build per-anchor enrichment tasks for findings missing context payloads.

    Returns:
    -------
    list[RenderEnrichmentTask]
        Unique per-anchor tasks requiring enrichment.
    """
    tasks: list[RenderEnrichmentTask] = []
    seen: set[tuple[str, int, int, str]] = set()
    for finding in iter_result_findings(result):
        anchor = finding.anchor
        if anchor is None:
            continue
        if allowed_files is not None and anchor.file not in allowed_files:
            continue
        col = int(anchor.col or 0)
        language = infer_language(finding)
        cache_key = (anchor.file, anchor.line, col, language)
        if cache_key in cache or cache_key in seen:
            continue
        needs_context = not isinstance(finding.details.get("context_snippet"), str)
        needs_enrichment = extract_enrichment_payload(finding) is None
        if not (needs_context or needs_enrichment):
            continue
        seen.add(cache_key)
        tasks.append(
            RenderEnrichmentTask(
                root=str(root),
                file=anchor.file,
                line=anchor.line,
                col=col,
                language=language,
                candidates=tuple(extract_match_text_candidates(finding)),
            )
        )
    return tasks


def _resolve_worker_count(task_count: int) -> int:
    if task_count <= 1:
        return 1
    return min(task_count, MAX_RENDER_ENRICH_WORKERS)


def populate_render_enrichment_cache(
    *,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    tasks: list[RenderEnrichmentTask],
    port: RenderEnrichmentPort | None,
) -> None:
    """Populate enrichment cache for prepared tasks."""
    workers = _resolve_worker_count(len(tasks))
    if workers <= 1:
        for task in tasks:
            result = _compute_payload_from_task(task=task, port=port)
            cache[result.file, result.line, result.col, result.language] = result.payload
        return

    scheduler = get_worker_scheduler()
    try:
        futures = [
            scheduler.submit_cpu(_compute_payload_from_task, task=task, port=port) for task in tasks
        ]
        batch = scheduler.collect_bounded(
            futures,
            timeout_seconds=max(1.0, float(len(tasks))),
        )
        if batch.timed_out > 0:
            for task in tasks:
                result = _compute_payload_from_task(task=task, port=port)
                cache[result.file, result.line, result.col, result.language] = result.payload
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
        for task in tasks:
            result = _compute_payload_from_task(task=task, port=port)
            cache[result.file, result.line, result.col, result.language] = result.payload


def precompute_render_enrichment_cache(
    *,
    result: CqResult,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    allowed_files: set[str] | None,
    port: RenderEnrichmentPort | None,
) -> list[RenderEnrichmentTask]:
    """Build tasks and precompute enrichment payloads into cache.

    Returns:
    -------
    list[RenderEnrichmentTask]
        Tasks that were computed and written to cache.
    """
    tasks = build_render_enrichment_tasks(
        result,
        root=root,
        cache=cache,
        allowed_files=allowed_files,
    )
    if not tasks:
        return []
    populate_render_enrichment_cache(cache=cache, tasks=tasks, port=port)
    return tasks


def count_render_enrichment_tasks(
    *,
    result: CqResult,
    root: Path,
    allowed_files: set[str] | None,
) -> int:
    """Count render-enrichment task volume without computing payloads.

    Returns:
    -------
    int
        Number of tasks that would be generated.
    """
    return len(
        build_render_enrichment_tasks(
            result,
            root=root,
            cache={},
            allowed_files=allowed_files,
        )
    )


def maybe_attach_render_enrichment(
    finding: Finding,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]] | None,
    allowed_files: set[str] | None,
    port: RenderEnrichmentPort | None,
) -> None:
    """Attach precomputed/on-demand render enrichment to finding details."""
    anchor = finding.anchor
    if anchor is None:
        return
    if allowed_files is not None and anchor.file not in allowed_files:
        return
    needs_context = not isinstance(finding.details.get("context_snippet"), str)
    needs_enrichment = extract_enrichment_payload(finding) is None
    if not (needs_context or needs_enrichment):
        return

    cache_key = (anchor.file, anchor.line, int(anchor.col or 0), infer_language(finding))
    payload: dict[str, object] | None = cache.get(cache_key) if cache is not None else None
    if payload is None:
        task = RenderEnrichmentTask(
            root=str(root),
            file=anchor.file,
            line=anchor.line,
            col=int(anchor.col or 0),
            language=infer_language(finding),
            candidates=tuple(extract_match_text_candidates(finding)),
        )
        result = _compute_payload_from_task(task=task, port=port)
        payload = result.payload
        if cache is not None:
            cache[cache_key] = payload
    merge_enrichment_details(finding, payload)


__all__ = [
    "RenderEnrichmentResult",
    "RenderEnrichmentTask",
    "count_render_enrichment_tasks",
    "maybe_attach_render_enrichment",
    "precompute_render_enrichment_cache",
    "select_enrichment_target_files",
]
