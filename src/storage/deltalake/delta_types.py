"""Shared Delta dataclass/type exports."""

from __future__ import annotations

from storage.deltalake.delta_read import (
    DeltaCdfOptions,
    DeltaDeleteWhereRequest,
    DeltaFeatureMutationOptions,
    DeltaMergeArrowRequest,
    DeltaReadRequest,
    DeltaSchemaRequest,
    DeltaVacuumOptions,
    DeltaWriteResult,
    IdempotentWriteOptions,
    SnapshotKey,
)

__all__ = [
    "DeltaCdfOptions",
    "DeltaDeleteWhereRequest",
    "DeltaFeatureMutationOptions",
    "DeltaMergeArrowRequest",
    "DeltaReadRequest",
    "DeltaSchemaRequest",
    "DeltaVacuumOptions",
    "DeltaWriteResult",
    "IdempotentWriteOptions",
    "SnapshotKey",
]
