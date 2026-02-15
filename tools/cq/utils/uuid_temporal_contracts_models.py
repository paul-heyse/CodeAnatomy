"""Structured contracts for UUID temporal semantics in CQ."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class TemporalUuidInfoV1(CqStruct, frozen=True):
    """Normalized metadata for one UUID value."""

    value: str
    version: int
    variant: str
    time_ms: int | None = None


class RunIdentityContractV1(CqStruct, frozen=True):
    """Run identity contract with UUID temporal fields."""

    run_id: str
    run_uuid_version: int
    run_variant: str
    run_created_ms: int


__all__ = [
    "RunIdentityContractV1",
    "TemporalUuidInfoV1",
]
