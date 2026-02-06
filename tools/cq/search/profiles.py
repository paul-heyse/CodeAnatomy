"""Search profile definitions with timeout and result limits.

Provides preset search limits for different use cases (interactive, audit, literal).
"""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class SearchLimits(CqStruct, frozen=True):
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

    max_files: int = 5000
    max_matches_per_file: int = 1000
    max_total_matches: int = 10000
    timeout_seconds: float = 30.0
    max_depth: int = 25
    max_file_size_bytes: int = 2 * 1024 * 1024


# Default limits for general-purpose searches
DEFAULT = SearchLimits()

# Interactive limits for fast user-facing queries
INTERACTIVE = SearchLimits(
    max_files=1000,
    timeout_seconds=10.0,
    max_depth=20,
    max_file_size_bytes=1 * 1024 * 1024,
)

# Audit limits for comprehensive codebase scans
AUDIT = SearchLimits(
    max_files=50000,
    max_total_matches=100000,
    timeout_seconds=300.0,
    max_depth=50,
    max_file_size_bytes=10 * 1024 * 1024,
)

# Literal limits for simple string searches
LITERAL = SearchLimits(
    max_files=2000,
    max_matches_per_file=500,
    max_depth=25,
    max_file_size_bytes=2 * 1024 * 1024,
)
