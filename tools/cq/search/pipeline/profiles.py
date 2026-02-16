"""Search profile definitions with timeout and result limits.

Provides preset search limits for different use cases (interactive, audit, literal).
"""

from __future__ import annotations

from tools.cq.search._shared.types import SearchLimits

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
