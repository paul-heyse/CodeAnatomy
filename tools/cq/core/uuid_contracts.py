"""CQ UUID identity contracts for run/artifact correlation."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class UuidIdentityContractV1(CqStruct, frozen=True):
    """Sortable UUID contract for CQ runtime identity fields."""

    run_id: str
    artifact_id: str
    cache_key_uses_uuid: bool = False
    run_uuid_version: int | None = None
    run_created_ms: int | None = None


__all__ = ["UuidIdentityContractV1"]
