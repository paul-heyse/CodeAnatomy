"""Tests for tree-sitter changed-range work queue helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.contracts.core_models import QueryWindowV1
from tools.cq.search.tree_sitter.core.work_queue import dequeue_window, enqueue_windows

FIRST_WINDOW_END_BYTE = 5
SECOND_WINDOW_START_BYTE = 10
SECOND_WINDOW_END_BYTE = 20


class _Cache:
    def __init__(self) -> None:
        self.items: list[bytes] = []

    def push(self, value: bytes, prefix: str | None = None) -> None:
        _ = prefix
        self.items.append(value)

    def pull(self, prefix: str | None = None) -> bytes | None:
        _ = prefix
        if not self.items:
            return None
        return self.items.pop(0)


class _FanoutLike:
    def __init__(self) -> None:
        self.named: dict[str, _Cache] = {}

    def cache(self, name: str) -> _Cache:
        store = self.named.get(name)
        if store is None:
            store = _Cache()
            self.named[name] = store
        return store


def test_enqueue_and_dequeue_windows_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test enqueue and dequeue windows roundtrip."""
    import tools.cq.search.tree_sitter.core.work_queue as queue_mod

    cache = _Cache()
    monkeypatch.setattr(queue_mod, "_cache", lambda: cache)
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
    """Test enqueue dequeue windows with named subcache."""
    import tools.cq.search.tree_sitter.core.work_queue as queue_mod

    cache = _FanoutLike()
    monkeypatch.setattr(queue_mod, "_cache", lambda: cache)
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
