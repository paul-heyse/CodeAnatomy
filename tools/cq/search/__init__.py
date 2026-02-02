"""Search module for rpygrep integration.

Provides timeout utilities, search profiles, and adapter functions for code search.
"""

from __future__ import annotations

from tools.cq.search.adapter import (
    find_call_candidates,
    find_callers,
    find_files_with_pattern,
    search_content,
)
from tools.cq.search.profiles import AUDIT, DEFAULT, INTERACTIVE, LITERAL, SearchLimits
from tools.cq.search.timeout import search_async_with_timeout, search_sync_with_timeout

__all__ = [
    "SearchLimits",
    "DEFAULT",
    "INTERACTIVE",
    "AUDIT",
    "LITERAL",
    "search_sync_with_timeout",
    "search_async_with_timeout",
    "find_files_with_pattern",
    "find_call_candidates",
    "find_callers",
    "search_content",
]
