"""Shared cache record converters and deterministic sort strategies."""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.schema import Finding
from tools.cq.search.cache.contracts import SgRecordCacheV1


def record_to_cache_record(record: SgRecord) -> SgRecordCacheV1:
    """Convert an ``SgRecord`` into cache-serializable form.

    Returns:
        SgRecordCacheV1: Cache-safe representation of the input record.
    """
    return SgRecordCacheV1(
        record=record.record,
        kind=record.kind,
        file=record.file,
        start_line=record.start_line,
        start_col=record.start_col,
        end_line=record.end_line,
        end_col=record.end_col,
        text=record.text,
        rule_id=record.rule_id,
    )


def cache_record_to_record(payload: SgRecordCacheV1) -> SgRecord:
    """Rehydrate runtime ``SgRecord`` from cache payload.

    Returns:
        SgRecord: Runtime record reconstructed from cache payload.
    """
    return SgRecord(
        record=payload.record,
        kind=payload.kind,
        file=payload.file,
        start_line=payload.start_line,
        start_col=payload.start_col,
        end_line=payload.end_line,
        end_col=payload.end_col,
        text=payload.text,
        rule_id=payload.rule_id,
    )


def record_sort_key_detailed(record: SgRecord) -> tuple[str, int, int, str, str, str, str]:
    """Deterministic high-detail sort key used by entity executor flows.

    Returns:
        tuple[str, int, int, str, str, str, str]: Stable detailed ordering key.
    """
    return (
        record.file,
        int(record.start_line),
        int(record.start_col),
        record.record,
        record.kind,
        record.rule_id,
        record.text,
    )


def finding_sort_key_detailed(finding: Finding) -> tuple[str, int, int, str]:
    """Deterministic high-detail finding sort key for entity execution.

    Returns:
        tuple[str, int, int, str]: Stable detailed ordering key for findings.
    """
    if finding.anchor is None:
        return ("", 0, 0, finding.message)
    return (
        finding.anchor.file,
        int(finding.anchor.line),
        int(finding.anchor.col or 0),
        finding.message,
    )


def record_sort_key_lightweight(record: SgRecord) -> tuple[str, int, int]:
    """Lightweight record sort key used by ast-grep pattern execution.

    Returns:
        tuple[str, int, int]: Stable lightweight ordering key for records.
    """
    return (record.file, record.start_line, record.start_col)


def finding_sort_key_lightweight(finding: Finding) -> tuple[str, int, int]:
    """Lightweight finding sort key used by ast-grep pattern execution.

    Returns:
        tuple[str, int, int]: Stable lightweight ordering key for findings.
    """
    if finding.anchor is None:
        return ("", 0, 0)
    return (finding.anchor.file, finding.anchor.line, finding.anchor.col or 0)


__all__ = [
    "cache_record_to_record",
    "finding_sort_key_detailed",
    "finding_sort_key_lightweight",
    "record_sort_key_detailed",
    "record_sort_key_lightweight",
    "record_to_cache_record",
]
