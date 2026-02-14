"""Shared contracts for fragment-oriented cache orchestration."""

from __future__ import annotations

from typing import Annotated

import msgspec

from tools.cq.core.structs import CqStruct

NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]


class FragmentRequestV1(CqStruct, frozen=True):
    """Execution envelope for fragment-cache orchestration."""

    namespace: str
    workspace: str
    language: str
    ttl_seconds: NonNegativeInt = 0
    tag: str | None = None
    scope_hash: str | None = None
    snapshot_digest: str | None = None
    run_id: str | None = None


class FragmentEntryV1(CqStruct, frozen=True):
    """One cache addressable fragment entry."""

    file: str
    cache_key: str
    content_hash: str = ""


class FragmentHitV1(CqStruct, frozen=True):
    """Decoded fragment cache hit payload."""

    entry: FragmentEntryV1
    payload: object


class FragmentMissV1(CqStruct, frozen=True):
    """Fragment cache miss descriptor."""

    entry: FragmentEntryV1


class FragmentPartitionV1(CqStruct, frozen=True):
    """Partitioned cache probe result."""

    hits: tuple[FragmentHitV1, ...] = ()
    misses: tuple[FragmentMissV1, ...] = ()


class FragmentWriteV1(CqStruct, frozen=True):
    """One fragment payload to persist into cache."""

    entry: FragmentEntryV1
    payload: object


__all__ = [
    "FragmentEntryV1",
    "FragmentHitV1",
    "FragmentMissV1",
    "FragmentPartitionV1",
    "FragmentRequestV1",
    "FragmentWriteV1",
]
