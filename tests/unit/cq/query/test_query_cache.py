"""Tests for query cache read-through helpers."""

from __future__ import annotations

from pathlib import Path

import msgspec
from tools.cq.query.query_cache import cached_scan


class _Payload(msgspec.Struct):
    value: int


class _FakeBackend:
    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.writes: list[tuple[str, object]] = []

    def get(self, key: str) -> object | None:
        return self.store.get(key)

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        _ = (expire, tag)
        self.store[key] = value
        self.writes.append((key, value))
        return True


def test_cached_scan_returns_cached_value_when_decodable(tmp_path: Path) -> None:
    backend = _FakeBackend()
    backend.store["k"] = msgspec.msgpack.encode(_Payload(value=7))

    calls = {"scan": 0}

    def _scan() -> _Payload:
        calls["scan"] += 1
        return _Payload(value=99)

    result = cached_scan(
        root=tmp_path,
        cache_key="k",
        scan_fn=_scan,
        type_=_Payload,
        backend=backend,
    )

    assert result == _Payload(value=7)
    assert calls["scan"] == 0


def test_cached_scan_writes_result_on_cache_miss(tmp_path: Path) -> None:
    backend = _FakeBackend()

    calls = {"scan": 0}

    def _scan() -> _Payload:
        calls["scan"] += 1
        return _Payload(value=13)

    result = cached_scan(
        root=tmp_path,
        cache_key="miss",
        scan_fn=_scan,
        type_=_Payload,
        backend=backend,
        expire=60,
        tag="query",
    )

    assert result == _Payload(value=13)
    assert calls["scan"] == 1
    assert backend.writes

    decoded = msgspec.msgpack.decode(backend.store["miss"], type=_Payload)
    assert decoded == _Payload(value=13)
