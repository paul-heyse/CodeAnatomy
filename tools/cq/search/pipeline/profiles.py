"""Search profile definitions with timeout and result limits.

Provides preset search limits for different use cases (interactive, audit, literal).
"""

from __future__ import annotations

from tools.cq.core.contracts_constraints import PositiveFloat, PositiveInt
from tools.cq.core.structs import CqSettingsStruct


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

# CI limits for deterministic output ordering
CI = SearchLimits(
    max_files=50000,
    max_total_matches=100000,
    timeout_seconds=300.0,
    max_depth=50,
    max_file_size_bytes=10 * 1024 * 1024,
    sort_by_path=True,
)

# Literal limits for simple string searches
LITERAL = SearchLimits(
    max_files=2000,
    max_matches_per_file=500,
    max_depth=25,
    max_file_size_bytes=2 * 1024 * 1024,
)
