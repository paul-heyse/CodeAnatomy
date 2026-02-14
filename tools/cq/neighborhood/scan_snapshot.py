"""Public scan snapshot adapter for structural neighborhood collection.

Decouples structural collector from private query/executor internals.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.query.executor import ScanContext

import msgspec

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.structs import CqStruct
from tools.cq.neighborhood.cache_pipeline import (
    NeighborhoodScanRequestV1,
    build_neighborhood_records,
)


class ScanSnapshot(CqStruct, frozen=True):
    """Public adapter exposing only what neighborhood needs from ScanContext.

    Decouples structural collector from private query/executor internals.

    Parameters
    ----------
    def_records : tuple[SgRecord, ...]
        Definition records (functions, classes, etc.).
    call_records : tuple[SgRecord, ...]
        Call records.
    interval_index : object
        Interval index for containment queries (duck-typed).
    file_index : object
        Per-file interval index (duck-typed).
    calls_by_def : dict[str, tuple[SgRecord, ...]]
        Mapping from definition location to calls within that definition.
    """

    def_records: tuple[SgRecord, ...] = ()
    call_records: tuple[SgRecord, ...] = ()
    interval_index: object = None
    file_index: object = None
    calls_by_def: dict[str, tuple[SgRecord, ...]] = msgspec.field(default_factory=dict)

    @classmethod
    def from_scan_context(cls, ctx: ScanContext) -> ScanSnapshot:
        """Build snapshot from a ScanContext.

        Returns:
            ScanSnapshot: Snapshot adapter built from scan context records.
        """
        calls_by_def_keyed: dict[str, tuple[SgRecord, ...]] = {
            _record_key(def_rec): tuple(calls) for def_rec, calls in ctx.calls_by_def.items()
        }
        return cls(
            def_records=tuple(ctx.def_records),
            call_records=tuple(ctx.call_records),
            interval_index=ctx.interval_index,
            file_index=ctx.file_index,
            calls_by_def=calls_by_def_keyed,
        )

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> ScanSnapshot:
        """Build snapshot from raw SgRecords.

        Returns:
            ScanSnapshot: Snapshot adapter built from raw scan records.
        """
        from tools.cq.query.executor import build_scan_context

        ctx = build_scan_context(records)
        return cls.from_scan_context(ctx)

    @classmethod
    def build_from_repo(
        cls,
        root: Path,
        lang: str = "auto",
        run_id: str | None = None,
    ) -> ScanSnapshot:
        """Build snapshot from repository scan using cached fragments/snapshots.

        Returns:
            ScanSnapshot: Snapshot loaded from cache when available or a fresh scan.
        """
        records = build_neighborhood_records(
            NeighborhoodScanRequestV1(
                root=str(root.resolve()),
                language=lang,
                run_id=run_id,
            )
        )
        return cls.from_records(records)


def _record_key(record: SgRecord) -> str:
    """Build a deterministic key for a record.

    Returns:
        str: Deterministic key for the record.
    """
    return f"{record.file}:{record.start_line}:{record.start_col}"


__all__ = ["ScanSnapshot"]
