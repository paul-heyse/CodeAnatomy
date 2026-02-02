"""Search profile definitions with timeout and result limits.

Provides preset search limits for different use cases (interactive, audit, literal).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SearchLimits:
    """Search limits for rpygrep operations.

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

    """

    max_files: int = 5000
    max_matches_per_file: int = 1000
    max_total_matches: int = 10000
    timeout_seconds: float = 30.0


# Default limits for general-purpose searches
DEFAULT = SearchLimits()

# Interactive limits for fast user-facing queries
INTERACTIVE = SearchLimits(max_files=1000, timeout_seconds=10.0)

# Audit limits for comprehensive codebase scans
AUDIT = SearchLimits(
    max_files=50000,
    max_total_matches=100000,
    timeout_seconds=300.0,
)

# Literal limits for simple string searches
LITERAL = SearchLimits(max_files=2000, max_matches_per_file=500)
