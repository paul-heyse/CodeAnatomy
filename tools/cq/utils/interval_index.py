"""Interval index utilities for span containment queries."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file


@dataclass
class IntervalIndex:
    """Index for efficient interval containment queries.

    Enables O(log n) lookup of which definition contains a given position.
    """

    # Sorted list of (start_line, end_line, record) tuples
    intervals: list[tuple[int, int, SgRecord]]

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> IntervalIndex:
        """Build interval index from definition records.

        Used by classification and query expansion to index definitions.

        Returns
        -------
        IntervalIndex
            Interval index for the provided records.
        """
        intervals = [(r.start_line, r.end_line, r) for r in records]
        # Sort by start line, then by end line (larger spans first for nesting)
        intervals.sort(key=lambda x: (x[0], -x[1]))
        return cls(intervals=intervals)

    def find_containing(self, line: int) -> SgRecord | None:
        """Find the innermost definition containing the given line.

        Used by search classifiers to attribute matches to definitions.

        Returns
        -------
        SgRecord | None
            Innermost containing record, or None if no match.
        """
        candidates = self.find_candidates(line)
        if not candidates:
            return None
        return min(candidates, key=lambda r: r.end_line - r.start_line)

    def find_candidates(self, line: int) -> list[SgRecord]:
        """Find all records whose span contains the given line.

        Used by ``find_containing`` to evaluate nested candidates.

        Returns
        -------
        list[SgRecord]
            Candidate records spanning the line.
        """
        candidates: list[SgRecord] = []
        for start, end, record in self.intervals:
            if start <= line <= end:
                candidates.append(record)
            elif start > line:
                break
        return candidates


@dataclass(frozen=True)
class FileIntervalIndex:
    """Per-file interval indexes to avoid cross-file attribution."""

    by_file: dict[str, IntervalIndex]

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> FileIntervalIndex:
        """Build per-file interval indexes.

        Used by query expansion to avoid cross-file attribution.

        Returns
        -------
        FileIntervalIndex
            Per-file interval index mapping.
        """
        grouped = group_records_by_file(records)
        return cls(
            by_file={
                file_path: IntervalIndex.from_records(recs) for file_path, recs in grouped.items()
            }
        )

    def find_containing(self, record: SgRecord) -> SgRecord | None:
        """Find the innermost definition containing the record.

        Used by call/callee expansion to locate enclosing definitions.

        Returns
        -------
        SgRecord | None
            Containing definition record, or None if not found.
        """
        index = self.by_file.get(record.file)
        if index is None:
            return None
        return index.find_containing(record.start_line)


__all__ = ["FileIntervalIndex", "IntervalIndex"]
