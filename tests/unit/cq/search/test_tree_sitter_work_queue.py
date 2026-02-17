"""Tests for tree-sitter changed-range work queue helpers."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1
from tools.cq.search.tree_sitter.core.work_queue import dequeue_window, enqueue_windows

FIRST_WINDOW_END_BYTE = 5
SECOND_WINDOW_START_BYTE = 10
SECOND_WINDOW_END_BYTE = 20


class _Backend:
    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._items: dict[str, bytes] = {}

    def incr(self, key: str, *, delta: int = 1, default: int = 0) -> int:
        current = self._counters.get(key, default)
        updated = int(current) + int(delta)
        self._counters[key] = updated
        return updated

    def set(
        self,
        key: str,
        value: bytes,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = expire
        _ = tag
        self._items[key] = value
        return True

    def get(self, key: str) -> bytes | None:
        return self._items.get(key)


def test_enqueue_and_dequeue_windows_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test enqueue and dequeue windows roundtrip."""
    import tools.cq.search.tree_sitter.core.work_queue as queue_mod

    backend = _Backend()
    monkeypatch.setattr(queue_mod, "get_cq_cache_backend", lambda **_kwargs: backend)
    pushed = enqueue_windows(
        language="python",
        file_key="sample.py",
        windows=(QueryWindowV1(start_byte=1, end_byte=5),),
    )
    assert pushed == 1
    item = dequeue_window("python")
    assert item is not None
    assert item.file_key == "sample.py"
    assert item.start_byte == 1
    assert item.end_byte == FIRST_WINDOW_END_BYTE


def test_enqueue_dequeue_windows_with_named_subcache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test dequeue path accepts memoryview payloads from backend adapters."""
    import tools.cq.search.tree_sitter.core.work_queue as queue_mod

    backend = _Backend()
    original_get: Callable[[str], bytes | None] = backend.get

    def _get_as_memoryview(key: str) -> memoryview | None:
        payload = original_get(key)
        return None if payload is None else memoryview(payload)

    monkeypatch.setattr(queue_mod, "get_cq_cache_backend", lambda **_kwargs: backend)
    monkeypatch.setattr(backend, "get", _get_as_memoryview)
    pushed = enqueue_windows(
        language="python",
        file_key="sample.py",
        windows=(QueryWindowV1(start_byte=10, end_byte=20),),
    )
    assert pushed == 1
    item = dequeue_window("python")
    assert item is not None
    assert item.file_key == "sample.py"
    assert item.start_byte == SECOND_WINDOW_START_BYTE
    assert item.end_byte == SECOND_WINDOW_END_BYTE
