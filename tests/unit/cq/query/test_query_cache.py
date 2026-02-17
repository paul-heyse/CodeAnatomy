"""Tests for shared query cache helpers."""

from __future__ import annotations

from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import cast

import msgspec
import pytest
from tools.cq.core.cache.fragment_contracts import FragmentMissV1, FragmentWriteV1
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.query.query_cache import (
    CachedScanRequest,
    QueryFragmentCacheContext,
    build_query_fragment_entries,
    cached_scan,
    run_query_fragment_scan,
)


class _Payload(msgspec.Struct):
    value: int


class _FakeBackend:
    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.writes: list[tuple[str, object, int | None, str | None]] = []

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
        self.store[key] = value
        self.writes.append((key, value, expire, tag))
        return True

    def set_many(
        self,
        items: dict[str, object],
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> int:
        for key, value in items.items():
            self.store[key] = value
            self.writes.append((key, value, expire, tag))
        return len(items)

    @staticmethod
    def transact() -> AbstractContextManager[None]:
        return nullcontext()


def test_cached_scan_returns_cached_value_when_decodable(tmp_path: Path) -> None:
    """Return cached payload when decode succeeds."""
    backend = _FakeBackend()
    backend.store["k"] = msgspec.msgpack.encode(_Payload(value=7))

    calls = {"scan": 0}

    def _scan() -> _Payload:
        calls["scan"] += 1
        return _Payload(value=99)

    result = cached_scan(
        request=CachedScanRequest(
            root=tmp_path,
            cache_key="k",
            backend=cast("CqCacheBackend", backend),
        ),
        scan_fn=_scan,
        type_=_Payload,
    )

    assert result == _Payload(value=7)
    assert calls["scan"] == 0


def test_cached_scan_writes_result_on_cache_miss(tmp_path: Path) -> None:
    """Write fresh scan payload into cache after miss."""
    backend = _FakeBackend()

    calls = {"scan": 0}

    def _scan() -> _Payload:
        calls["scan"] += 1
        return _Payload(value=13)

    result = cached_scan(
        request=CachedScanRequest(
            root=tmp_path,
            cache_key="miss",
            backend=cast("CqCacheBackend", backend),
            expire=60,
            tag="query",
        ),
        scan_fn=_scan,
        type_=_Payload,
    )

    assert result == _Payload(value=13)
    assert calls["scan"] == 1
    assert backend.writes

    cached_payload = backend.store["miss"]
    assert isinstance(cached_payload, (bytes, bytearray, memoryview))
    decoded = msgspec.msgpack.decode(cached_payload, type=_Payload)
    assert decoded == _Payload(value=13)


def test_run_query_fragment_scan_reuses_cached_fragment_hits(tmp_path: Path) -> None:
    """Reuse decoded hit payload on subsequent fragment scans."""
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    file_path = root / "module.py"
    file_path.write_text("def alpha() -> int:\n    return 1\n", encoding="utf-8")

    backend = _FakeBackend()
    context = QueryFragmentCacheContext(
        namespace="query_entity_fragment",
        root=root,
        language="python",
        files=[file_path],
        cache=cast("CqCacheBackend", backend),
        cache_enabled=True,
        ttl_seconds=120,
        tag="tag",
    )
    entries = build_query_fragment_entries(context)
    expected_file = entries[0].file

    calls = {"scan_misses": 0}

    def _decode(payload: object) -> object | None:
        if not isinstance(payload, dict):
            return None
        rows = payload.get("rows")
        if not isinstance(rows, list):
            return None
        if not all(isinstance(item, str) for item in rows):
            return None
        return rows

    def _scan_misses(
        misses: list[FragmentMissV1],
    ) -> tuple[dict[str, list[str]], list[FragmentWriteV1]]:
        calls["scan_misses"] += 1
        payload = {miss.entry.file: [miss.entry.file] for miss in misses}
        writes = [
            FragmentWriteV1(entry=miss.entry, payload={"rows": [miss.entry.file]})
            for miss in misses
        ]
        return payload, writes

    first = run_query_fragment_scan(
        context=context,
        entries=entries,
        run_id="run-1",
        decode=_decode,
        scan_misses=_scan_misses,
    )

    assert calls["scan_misses"] == 1
    assert len(first.misses) == 1
    assert first.miss_payload == {expected_file: [expected_file]}

    second = run_query_fragment_scan(
        context=context,
        entries=entries,
        run_id="run-2",
        decode=_decode,
        scan_misses=lambda misses: pytest.fail(f"unexpected misses: {misses}"),
    )

    assert len(second.hits) == 1
    assert second.miss_payload is None
    assert second.hits[0].entry.file == expected_file
    assert second.hits[0].payload == [expected_file]
