"""Foundation vocabulary types for search subsystem.

Defines core enums and configuration types used across search pipeline layers.
"""

from __future__ import annotations

from enum import Enum

from tools.cq.core.contracts_constraints import PositiveFloat, PositiveInt
from tools.cq.core.structs import CqSettingsStruct


class QueryMode(Enum):
    """Search query mode classification."""

    IDENTIFIER = "identifier"  # Word boundary match
    REGEX = "regex"  # User-provided regex
    LITERAL = "literal"  # Exact string match


class SearchLimits(CqSettingsStruct, frozen=True):
    """Search limits for ripgrep operations.

    Parameters
    ----------
    max_files : int, default=5000
        Maximum number of files to scan
    max_matches_per_file : int, default=1000
        Maximum matches to return per file
    max_total_matches : int, default=10000
        Maximum total matches across all files
    timeout_seconds : float, default=30.0
        Maximum execution time in seconds
    max_depth : int, default=25
        Maximum directory traversal depth
    max_file_size_bytes : int, default=2097152
        Maximum file size to scan (in bytes)

    """

    max_files: PositiveInt = 5000
    max_matches_per_file: PositiveInt = 1000
    max_total_matches: PositiveInt = 10000
    timeout_seconds: PositiveFloat = 30.0
    max_depth: PositiveInt = 25
    max_file_size_bytes: PositiveInt = 2 * 1024 * 1024
    context_before: int = 0
    context_after: int = 0
    multiline: bool = False
    sort_by_path: bool = False


__all__ = ["QueryMode", "SearchLimits"]
