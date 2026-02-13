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

        Parameters
        ----------
        ctx : ScanContext
            Query execution scan context.

        Returns:
        -------
        ScanSnapshot
            Snapshot adapter for neighborhood collection.
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

        Uses query scan-context builder to construct indexes.

        Parameters
        ----------
        records : list[SgRecord]
            Raw scan records.

        Returns:
        -------
        ScanSnapshot
            Snapshot adapter for neighborhood collection.
        """
        from tools.cq.query.executor import build_scan_context

        ctx = build_scan_context(records)
        return cls.from_scan_context(ctx)

    @classmethod
    def build_from_repo(cls, root: Path, lang: str = "auto") -> ScanSnapshot:
        """Build snapshot from repository scan.

        This is the recommended pattern: sg_scan → from_records.

        Parameters
        ----------
        root : Path
            Repository root path.
        lang : str
            Query language (default: "auto").

        Returns:
        -------
        ScanSnapshot
            Snapshot from repository scan.
        """
        from tools.cq.query.language import QueryLanguage
        from tools.cq.query.sg_parser import sg_scan

        # Type-narrow lang to QueryLanguage
        query_lang: QueryLanguage = lang if lang in {"python", "rust"} else "python"  # type: ignore[assignment]
        records = sg_scan(paths=[root], lang=query_lang, root=root)
        return cls.from_records(records)


def _record_key(record: SgRecord) -> str:
    """Build a deterministic key for a record.

    Parameters
    ----------
    record : SgRecord
        Record to key.

    Returns:
    -------
    str
        Key string (file:start_line:start_col).
    """
    return f"{record.file}:{record.start_line}:{record.start_col}"


__all__ = ["ScanSnapshot"]
