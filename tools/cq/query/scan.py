"""Scan context management for query execution.

Builds scan contexts from ast-grep records with interval indexing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.query.sg_parser import filter_records_by_kind
from tools.cq.utils.interval_index import FileIntervalIndex, IntervalIndex


@dataclass(frozen=True, slots=True)
class ScanContext:
    """Bundled context from ast-grep scan for query processing."""

    def_records: tuple[SgRecord, ...]
    call_records: tuple[SgRecord, ...]
    interval_index: IntervalIndex[SgRecord]
    file_index: FileIntervalIndex
    calls_by_def: Mapping[SgRecord, tuple[SgRecord, ...]]
    all_records: tuple[SgRecord, ...]


@dataclass(frozen=True, slots=True)
class EntityCandidates:
    """Candidate record buckets for entity queries."""

    def_records: tuple[SgRecord, ...]
    import_records: tuple[SgRecord, ...]
    call_records: tuple[SgRecord, ...]


def build_scan_context(records: list[SgRecord]) -> ScanContext:
    """Build scan context from ast-grep records.

    Parameters
    ----------
    records
        AST-grep records to index

    Returns:
    -------
    ScanContext
        Scan context with interval indexes and call assignments
    """
    def_records = filter_records_by_kind(records, "def")
    interval_index = IntervalIndex.from_records(def_records)
    file_index = FileIntervalIndex.from_records(def_records)
    call_records = filter_records_by_kind(records, "call")
    calls_by_def = MappingProxyType(assign_calls_to_defs(interval_index, call_records))
    return ScanContext(
        def_records=tuple(def_records),
        call_records=tuple(call_records),
        interval_index=interval_index,
        file_index=file_index,
        calls_by_def=calls_by_def,
        all_records=tuple(records),
    )


def build_entity_candidates(scan: ScanContext, records: list[SgRecord]) -> EntityCandidates:
    """Build entity candidates from scan context.

    Parameters
    ----------
    scan
        Scan context with indexed records
    records
        All ast-grep records

    Returns:
    -------
    EntityCandidates
        Candidate buckets by entity type
    """
    return EntityCandidates(
        def_records=scan.def_records,
        import_records=tuple(filter_records_by_kind(records, "import")),
        call_records=scan.call_records,
    )


def assign_calls_to_defs(
    index: IntervalIndex[SgRecord],
    calls: list[SgRecord],
) -> dict[SgRecord, tuple[SgRecord, ...]]:
    """Assign call records to their containing definitions.

    Parameters
    ----------
    index
        Interval index of definitions
    calls
        Call records to assign

    Returns:
    -------
    dict[SgRecord, tuple[SgRecord, ...]]
        Mapping from definition to calls within it
    """
    from tools.cq.astgrep.sgpy_scanner import group_records_by_file

    result: dict[SgRecord, list[SgRecord]] = {}

    # Group calls by file
    calls_by_file = group_records_by_file(calls)

    # Group defs by file
    defs_by_file: dict[str, list[SgRecord]] = {}
    for _start, _end, record in index.intervals:
        if record.file not in defs_by_file:
            defs_by_file[record.file] = []
        defs_by_file[record.file].append(record)

    # For each file, build local index and assign
    for file_path, file_calls in calls_by_file.items():
        file_defs = defs_by_file.get(file_path, [])
        if not file_defs:
            continue

        # Build local index
        local_index = IntervalIndex.from_records(file_defs)

        # Assign each call
        for call in file_calls:
            containing_def = local_index.find_containing(call.start_line)
            if containing_def:
                if containing_def not in result:
                    result[containing_def] = []
                result[containing_def].append(call)

    return {record: tuple(call_records) for record, call_records in result.items()}


__all__ = [
    "EntityCandidates",
    "ScanContext",
    "assign_calls_to_defs",
    "build_entity_candidates",
    "build_scan_context",
]
