"""Helpers to derive bounded query windows from tree-sitter changed ranges."""

from __future__ import annotations

from tools.cq.search.tree_sitter_change_windows_contracts import QueryWindowSourceV1
from tools.cq.search.tree_sitter_runtime_contracts import QueryWindowV1
from tools.cq.search.tree_sitter_work_queue import enqueue_windows


def _window_span(row: object) -> tuple[int, int]:
    start = getattr(row, "start_byte", None)
    end = getattr(row, "end_byte", None)
    if not isinstance(start, int) or not isinstance(end, int):
        return (-1, -1)
    return start, end


def _merge_windows(windows: list[tuple[int, int]]) -> tuple[QueryWindowV1, ...]:
    if not windows:
        return ()
    windows.sort()
    merged: list[tuple[int, int]] = []
    current_start, current_end = windows[0]
    for start, end in windows[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
            continue
        merged.append((current_start, current_end))
        current_start, current_end = start, end
    merged.append((current_start, current_end))
    return tuple(QueryWindowV1(start_byte=start, end_byte=end) for start, end in merged)


def windows_from_changed_ranges(
    changed_ranges: tuple[object, ...] | None,
    *,
    source_byte_len: int,
    pad_bytes: int = 64,
) -> tuple[QueryWindowV1, ...]:
    """Build merged query windows from incremental changed-range rows.

    Returns:
        tuple[QueryWindowV1, ...]: Query windows clamped to source bounds.
    """
    if source_byte_len <= 0 or not changed_ranges:
        return ()

    normalized: list[tuple[int, int]] = []
    pad = max(0, int(pad_bytes))
    for change in changed_ranges:
        start, end = _window_span(change)
        if start < 0 or end <= start:
            continue
        clamped_start = max(0, start - pad)
        clamped_end = min(source_byte_len, end + pad)
        if clamped_end > clamped_start:
            normalized.append((clamped_start, clamped_end))
    return _merge_windows(normalized)


def ensure_query_windows(
    primary: tuple[QueryWindowV1, ...],
    *,
    fallback: QueryWindowV1 | None = None,
) -> tuple[QueryWindowV1, ...]:
    """Return primary windows, or a fallback window when primary is empty."""
    windows = tuple(primary)
    if windows:
        return windows
    if fallback is None:
        return ()
    return (fallback,)


def contains_window(windows: tuple[QueryWindowV1, ...], *, value: int, width: int = 0) -> bool:
    """Return whether a target byte offset range is covered by any window.

    Returns:
        bool: True when any window overlaps the requested byte range.
    """
    target_end = value + max(0, width)
    for window in windows:
        start = int(window.start_byte)
        end = int(window.end_byte)
        if value >= start and value <= end:
            return True
        if target_end and target_end >= start and target_end <= end:
            return True
        if value <= start and target_end >= end:
            return True
    return False


def to_query_window_source(windows: tuple[QueryWindowV1, ...]) -> tuple[QueryWindowSourceV1, ...]:
    """Convert runtime windows to serialization contracts.

    Returns:
        tuple[QueryWindowSourceV1, ...]: Serializable query window rows.
    """
    return tuple(
        QueryWindowSourceV1(start_byte=int(window.start_byte), end_byte=int(window.end_byte))
        for window in windows
    )


def queue_changed_range_windows(
    *,
    language: str,
    file_key: str,
    changed_ranges: tuple[object, ...] | None,
    source_byte_len: int,
    pad_bytes: int = 64,
) -> int:
    """Derive windows from changed ranges and enqueue them in the work queue.

    Returns:
        int: Number of queued window records.
    """
    windows = windows_from_changed_ranges(
        changed_ranges,
        source_byte_len=source_byte_len,
        pad_bytes=pad_bytes,
    )
    return enqueue_windows(language=language, file_key=file_key, windows=windows)


__all__ = [
    "contains_window",
    "ensure_query_windows",
    "queue_changed_range_windows",
    "to_query_window_source",
    "windows_from_changed_ranges",
]
