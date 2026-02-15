"""Diskcache-backed changed-range work queue helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import msgspec

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.search.tree_sitter_runtime_contracts import QueryWindowV1
from tools.cq.search.tree_sitter_work_queue_contracts import TreeSitterWorkItemV1


def _cache() -> Any | None:
    backend = get_cq_cache_backend(root=Path.cwd())
    return getattr(backend, "cache", None)


def _has_queue_ops(cache: Any) -> bool:
    members = dir(cache)
    return "push" in members and "pull" in members


def _queue_store(cache: Any, language: str) -> Any | None:
    if _has_queue_ops(cache):
        return cache
    cache_method = getattr(type(cache), "cache", None)
    if not callable(cache_method):
        return None
    cache_obj = cast("Any", cache)
    try:
        scoped_cache = cache_obj.cache(f"ts_work_queue:{language}")
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None
    if _has_queue_ops(scoped_cache):
        return scoped_cache
    return None


def _queue_prefix(language: str) -> str:
    return f"ts_work_queue:{language}:"


def enqueue_windows(
    *,
    language: str,
    file_key: str,
    windows: tuple[QueryWindowV1, ...],
) -> int:
    """Enqueue changed-range windows for follow-up processing.

    Returns:
        int: Number of windows pushed into the queue.
    """
    cache = _cache()
    if cache is None:
        return 0
    queue = _queue_store(cache, language)
    if queue is None:
        return 0
    pushed = 0
    push_fn = getattr(queue, "push", None)
    if not callable(push_fn):
        return 0
    prefix = _queue_prefix(language)
    for window in windows:
        payload = msgspec.json.encode(
            TreeSitterWorkItemV1(
                language=language,
                file_key=file_key,
                start_byte=int(window.start_byte),
                end_byte=int(window.end_byte),
            )
        )
        try:
            push_fn(payload, prefix=prefix)
            pushed += 1
        except TypeError:
            push_fn(payload)
            pushed += 1
    return pushed


def dequeue_window(language: str) -> TreeSitterWorkItemV1 | None:
    """Dequeue one changed-range work item for a language lane.

    Returns:
        TreeSitterWorkItemV1 | None: Dequeued work item when available.
    """
    cache = _cache()
    if cache is None:
        return None
    queue = _queue_store(cache, language)
    if queue is None:
        return None
    pull_fn = getattr(queue, "pull", None)
    if not callable(pull_fn):
        return None
    prefix = _queue_prefix(language)
    try:
        payload = pull_fn(prefix=prefix)
    except TypeError:
        payload = pull_fn()
    if isinstance(payload, tuple) and payload:
        payload = payload[-1]
    if not isinstance(payload, (bytes, bytearray)):
        return None
    try:
        return msgspec.json.decode(payload, type=TreeSitterWorkItemV1)
    except msgspec.DecodeError:
        return None


__all__ = ["dequeue_window", "enqueue_windows"]
