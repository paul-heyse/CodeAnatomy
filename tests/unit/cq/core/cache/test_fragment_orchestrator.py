"""Tests for shared fragment scan orchestration."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any, cast

import pytest
from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentHitV1,
    FragmentMissV1,
    FragmentPartitionV1,
    FragmentRequestV1,
    FragmentWriteV1,
)
from tools.cq.core.cache.fragment_engine import FragmentPersistRuntimeV1, FragmentProbeRuntimeV1
from tools.cq.core.cache.fragment_orchestrator import run_fragment_scan


def test_run_fragment_scan_scans_misses_and_persists(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fragment orchestrator scans misses and persists produced writes."""
    entry = FragmentEntryV1(file="a.py", cache_key="k1", content_hash="h1")
    request = FragmentRequestV1(
        namespace="ns",
        workspace=".",
        language="python",
    )

    def fake_partition(
        _request: FragmentRequestV1,
        _entries: list[FragmentEntryV1],
        _runtime: FragmentProbeRuntimeV1,
    ) -> FragmentPartitionV1:
        return FragmentPartitionV1(
            hits=(FragmentHitV1(entry=entry, payload={"hit": True}),),
            misses=(FragmentMissV1(entry=entry),),
        )

    persisted: list[FragmentWriteV1] = []

    def fake_persist(
        _request: FragmentRequestV1,
        writes: list[FragmentWriteV1],
        _runtime: FragmentPersistRuntimeV1,
    ) -> None:
        persisted.extend(writes)

    monkeypatch.setattr(
        "tools.cq.core.cache.fragment_orchestrator.partition_fragment_entries",
        fake_partition,
    )
    monkeypatch.setattr(
        "tools.cq.core.cache.fragment_orchestrator.persist_fragment_writes",
        fake_persist,
    )

    probe_runtime = FragmentProbeRuntimeV1(
        cache_get=lambda _key: None,
        decode=lambda payload: payload,
        cache_enabled=False,
        record_get=lambda **_kwargs: None,
        record_decode_failure=lambda **_kwargs: None,
    )
    persist_runtime = FragmentPersistRuntimeV1(
        cache_set=lambda *_args, **_kwargs: True,
        encode=lambda payload: payload,
        cache_enabled=False,
        transact=lambda: cast("Any", nullcontext()),
        record_set=lambda **_kwargs: None,
    )

    result = run_fragment_scan(
        request=request,
        entries=[entry],
        probe_runtime=probe_runtime,
        persist_runtime=persist_runtime,
        scan_misses=lambda misses: cast(
            "tuple[dict[str, Any], list[FragmentWriteV1]]",
            (
                {"misses": len(misses)},
                [FragmentWriteV1(entry=entry, payload={"cached": True})],
            ),
        ),
    )

    assert len(result.hits) == 1
    assert len(result.misses) == 1
    assert result.miss_payload == {"misses": 1}
    assert len(persisted) == 1
    assert persisted[0].payload == {"cached": True}
