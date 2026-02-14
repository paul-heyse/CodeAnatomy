"""Shared fragment-cache orchestration engine."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass

from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentHitV1,
    FragmentMissV1,
    FragmentPartitionV1,
    FragmentRequestV1,
    FragmentWriteV1,
)

DecodeFn = Callable[[object], object | None]
EncodeFn = Callable[[object], object]
CacheGetFn = Callable[[str], object | None]
CacheSetFn = Callable[..., bool]
TransactFactoryFn = Callable[[], AbstractContextManager[None]]
RecordGetFn = Callable[..., None]
RecordSetFn = Callable[..., None]
RecordDecodeFailureFn = Callable[..., None]


@dataclass(frozen=True, slots=True)
class FragmentProbeRuntimeV1:
    """Runtime function bundle for fragment cache probe operations."""

    cache_get: CacheGetFn
    decode: DecodeFn
    cache_enabled: bool
    record_get: RecordGetFn
    record_decode_failure: RecordDecodeFailureFn


@dataclass(frozen=True, slots=True)
class FragmentPersistRuntimeV1:
    """Runtime function bundle for fragment cache persistence operations."""

    cache_set: CacheSetFn
    encode: EncodeFn
    cache_enabled: bool
    transact: TransactFactoryFn
    record_set: RecordSetFn


def partition_fragment_entries(
    request: FragmentRequestV1,
    entries: list[FragmentEntryV1],
    runtime: FragmentProbeRuntimeV1,
) -> FragmentPartitionV1:
    """Partition fragment entries into cache hits and misses.

    Returns:
        FragmentPartitionV1: Cache hit/miss partition.
    """
    hits: list[FragmentHitV1] = []
    misses: list[FragmentMissV1] = []
    for entry in entries:
        if not (runtime.cache_enabled and entry.content_hash):
            misses.append(FragmentMissV1(entry=entry))
            continue
        cached = runtime.cache_get(entry.cache_key)
        runtime.record_get(request.namespace, isinstance(cached, dict), entry.cache_key)
        if not isinstance(cached, dict):
            misses.append(FragmentMissV1(entry=entry))
            continue
        decoded = runtime.decode(cached)
        if decoded is None:
            runtime.record_decode_failure(request.namespace)
            misses.append(FragmentMissV1(entry=entry))
            continue
        hits.append(FragmentHitV1(entry=entry, payload=decoded))
    return FragmentPartitionV1(hits=tuple(hits), misses=tuple(misses))


def persist_fragment_writes(
    request: FragmentRequestV1,
    writes: list[FragmentWriteV1],
    runtime: FragmentPersistRuntimeV1,
) -> None:
    """Persist fragment payload writes using one backend transaction.

    Returns:
        None
    """
    if not (runtime.cache_enabled and writes):
        return
    with runtime.transact():
        for write in writes:
            ok = runtime.cache_set(
                write.entry.cache_key,
                runtime.encode(write.payload),
                request.ttl_seconds,
                request.tag,
            )
            runtime.record_set(request.namespace, ok, write.entry.cache_key)


__all__ = [
    "FragmentPersistRuntimeV1",
    "FragmentProbeRuntimeV1",
    "partition_fragment_entries",
    "persist_fragment_writes",
]
