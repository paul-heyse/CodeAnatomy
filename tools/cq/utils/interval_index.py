"""Interval index utilities for span containment queries."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file


@dataclass(frozen=True)
class _IntervalNode[T]:
    center: int
    intervals: list[tuple[int, int, T]]
    left: _IntervalNode[T] | None
    right: _IntervalNode[T] | None


def _build_interval_tree[T](intervals: list[tuple[int, int, T]]) -> _IntervalNode[T] | None:
    if not intervals:
        return None

    center = intervals[len(intervals) // 2][0]
    left: list[tuple[int, int, T]] = []
    right: list[tuple[int, int, T]] = []
    overlapping: list[tuple[int, int, T]] = []

    for start, end, payload in intervals:
        if end < center:
            left.append((start, end, payload))
        elif start > center:
            right.append((start, end, payload))
        else:
            overlapping.append((start, end, payload))

    return _IntervalNode(
        center=center,
        intervals=overlapping,
        left=_build_interval_tree(left),
        right=_build_interval_tree(right),
    )


@dataclass
class IntervalIndex[T]:
    """Index for efficient interval containment queries.

    Enables O(log n) lookup of which definition contains a given position.
    """

    # Sorted list of (start_line, end_line, payload) tuples
    intervals: list[tuple[int, int, T]]
    _root: _IntervalNode[T] | None = None

    @classmethod
    def from_intervals(cls, intervals: list[tuple[int, int, T]]) -> IntervalIndex[T]:
        """Build interval index from explicit spans.

        Used by AST span indexes and record classification.

        Returns
        -------
        IntervalIndex
            Interval index for the provided spans.
        """
        sorted_intervals = sorted(intervals, key=lambda x: (x[0], -x[1]))
        return cls(
            intervals=sorted_intervals,
            _root=_build_interval_tree(sorted_intervals),
        )

    @classmethod
    def from_records(
        cls: type[IntervalIndex[SgRecord]],
        records: list[SgRecord],
    ) -> IntervalIndex[SgRecord]:
        """Build interval index from definition records.

        Used by classification and query expansion to index definitions.

        Returns
        -------
        IntervalIndex
            Interval index for the provided records.
        """
        intervals = [(r.start_line, r.end_line, r) for r in records]
        return cls.from_intervals(intervals)

    def find_containing(self, line: int) -> T | None:
        """Find the innermost definition containing the given line.

        Used by search classifiers to attribute matches to definitions.

        Returns
        -------
        T | None
            Innermost containing payload, or None if no match.
        """
        candidates = self._find_candidate_intervals(line)
        if not candidates:
            return None
        _, _, payload = min(
            candidates,
            key=lambda entry: (entry[1] - entry[0], entry[0], -entry[1]),
        )
        return payload

    def find_candidates(self, line: int) -> list[T]:
        """Find all records whose span contains the given line.

        Used by ``find_containing`` to evaluate nested candidates.

        Returns
        -------
        list[T]
            Candidate payloads spanning the line.
        """
        candidates = self._find_candidate_intervals(line)
        candidates.sort(key=lambda entry: (entry[0], -entry[1]))
        return [payload for _start, _end, payload in candidates]

    def _find_candidate_intervals(self, line: int) -> list[tuple[int, int, T]]:
        if self._root is None:
            return [
                (start, end, payload)
                for start, end, payload in self.intervals
                if start <= line <= end
            ]
        candidates: list[tuple[int, int, T]] = []
        _collect_candidates(self._root, line, candidates)
        return candidates


def _collect_candidates[T](
    node: _IntervalNode[T] | None,
    line: int,
    candidates: list[tuple[int, int, T]],
) -> None:
    if node is None:
        return

    if line < node.center:
        for start, end, payload in node.intervals:
            if start <= line <= end:
                candidates.append((start, end, payload))
        _collect_candidates(node.left, line, candidates)
        return
    if line > node.center:
        for start, end, payload in node.intervals:
            if start <= line <= end:
                candidates.append((start, end, payload))
        _collect_candidates(node.right, line, candidates)
        return

    candidates.extend(node.intervals)


@dataclass(frozen=True)
class FileIntervalIndex:
    """Per-file interval indexes to avoid cross-file attribution."""

    by_file: dict[str, IntervalIndex[SgRecord]]

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
