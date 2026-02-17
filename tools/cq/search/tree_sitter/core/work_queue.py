"""Cache-backed changed-range work queue helpers."""

from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1, TreeSitterWorkItemV1

_QUEUE_TTL_SECONDS = 900


def _queue_index_key(language: str) -> str:
    return f"cq:tree_sitter_work_queue:v1:{language}:next"


def _queue_head_key(language: str) -> str:
    return f"cq:tree_sitter_work_queue:v1:{language}:head"


def _queue_item_key(language: str, idx: int) -> str:
    return f"cq:tree_sitter_work_queue:v1:{language}:item:{idx}"


def enqueue_windows(
    *,
    language: str,
    file_key: str,
    windows: tuple[QueryWindowV1, ...],
) -> int:
    """Enqueue changed-range windows for follow-up processing.

    Returns:
        int: Number of windows enqueued.
    """
    backend = get_cq_cache_backend(root=Path.cwd())
    pushed = 0
    for window in windows:
        idx = backend.incr(_queue_index_key(language), delta=1, default=0)
        if idx is None:
            continue
        payload = msgspec.json.encode(
            TreeSitterWorkItemV1(
                language=language,
                file_key=file_key,
                start_byte=int(window.start_byte),
                end_byte=int(window.end_byte),
            )
        )
        ok = backend.set(
            _queue_item_key(language, int(idx)),
            payload,
            expire=_QUEUE_TTL_SECONDS,
            tag="ns:tree_sitter|kind:work_queue",
        )
        if ok:
            pushed += 1
    return pushed


def dequeue_window(language: str) -> TreeSitterWorkItemV1 | None:
    """Dequeue one changed-range work item for a language lane.

    Returns:
        TreeSitterWorkItemV1 | None: Next queued work item when available.
    """
    backend = get_cq_cache_backend(root=Path.cwd())
    idx = backend.incr(_queue_head_key(language), delta=1, default=0)
    if idx is None:
        return None
    payload = backend.get(_queue_item_key(language, int(idx)))
    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    elif isinstance(payload, bytearray):
        payload = bytes(payload)
    if not isinstance(payload, bytes):
        return None
    try:
        return msgspec.json.decode(payload, type=TreeSitterWorkItemV1)
    except msgspec.DecodeError:
        return None


__all__ = ["dequeue_window", "enqueue_windows"]
