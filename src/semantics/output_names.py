"""Shared canonical output names for semantic and relationship surfaces."""

from __future__ import annotations

from typing import Final

RELATION_OUTPUT_NAME: Final[str] = "relation_output"
RELATION_OUTPUT_ORDERING_KEYS: Final[tuple[tuple[str, str], ...]] = (
    ("path", "ascending"),
    ("bstart", "ascending"),
    ("bend", "ascending"),
    ("edge_owner_file_id", "ascending"),
    ("src", "ascending"),
    ("dst", "ascending"),
    ("task_priority", "ascending"),
    ("task_name", "ascending"),
)

__all__ = ["RELATION_OUTPUT_NAME", "RELATION_OUTPUT_ORDERING_KEYS"]
