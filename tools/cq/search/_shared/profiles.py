"""Shared search profile defaults used across search subsystems."""

from __future__ import annotations

from tools.cq.search._shared.types import SearchLimits

# Default limits for general-purpose searches.
DEFAULT = SearchLimits()

# Interactive limits for fast user-facing queries.
INTERACTIVE = SearchLimits(
    max_files=1000,
    timeout_seconds=10.0,
    max_depth=20,
    max_file_size_bytes=1 * 1024 * 1024,
)

__all__ = ["DEFAULT", "INTERACTIVE", "SearchLimits"]
