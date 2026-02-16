"""Shared orchestration wrapper around fragment probe/persist operations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentHitV1,
    FragmentMissV1,
    FragmentRequestV1,
    FragmentWriteV1,
)
from tools.cq.core.cache.fragment_engine import (
    FragmentPersistRuntimeV1,
    FragmentProbeRuntimeV1,
    partition_fragment_entries,
    persist_fragment_writes,
)


@dataclass(frozen=True, slots=True)
class FragmentScanResult[T]:
    """Result envelope for a probe->miss-scan->persist fragment workflow."""

    hits: tuple[FragmentHitV1, ...]
    misses: tuple[FragmentMissV1, ...]
    miss_payload: T | None = None


def run_fragment_scan[T](
    *,
    request: FragmentRequestV1,
    entries: list[FragmentEntryV1],
    probe_runtime: FragmentProbeRuntimeV1,
    persist_runtime: FragmentPersistRuntimeV1,
    scan_misses: Callable[[list[FragmentMissV1]], tuple[T, list[FragmentWriteV1]]],
) -> FragmentScanResult[T]:
    """Run one fragment cache cycle with shared miss handling orchestration.

    Returns:
    -------
    FragmentScanResult[T]
        Hit/miss partition plus optional miss-scan payload.
    """
    partition = partition_fragment_entries(request, entries, probe_runtime)
    miss_payload: T | None = None
    if partition.misses:
        miss_payload, writes = scan_misses(list(partition.misses))
        persist_fragment_writes(request, writes, persist_runtime)
    return FragmentScanResult(
        hits=partition.hits,
        misses=partition.misses,
        miss_payload=miss_payload,
    )


__all__ = ["FragmentScanResult", "run_fragment_scan"]
