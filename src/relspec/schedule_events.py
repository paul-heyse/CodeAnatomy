"""Scheduling metadata helpers for task execution."""

from __future__ import annotations

from serde_msgspec import StructBaseStrict


class TaskScheduleMetadata(StructBaseStrict, frozen=True):
    """Schedule metadata for a task execution event."""

    schedule_index: int
    generation_index: int
    generation_order: int
    generation_size: int


__all__ = ["TaskScheduleMetadata"]
