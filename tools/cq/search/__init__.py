"""Search module for native ripgrep integration.

Provides timeout utilities, search profiles, adapter functions, and smart search.
"""

from __future__ import annotations

from tools.cq.search.adapter import (
    find_call_candidates,
    find_callers,
    find_files_with_pattern,
    search_content,
)
from tools.cq.search.classifier import (
    MatchCategory,
    QueryMode,
    classify_from_node,
    classify_from_records,
    classify_heuristic,
    detect_query_mode,
    enrich_with_symtable,
)
from tools.cq.search.models import SearchConfig
from tools.cq.search.profiles import AUDIT, DEFAULT, INTERACTIVE, LITERAL, SearchLimits
from tools.cq.search.smart_search import (
    SMART_SEARCH_LIMITS,
    EnrichedMatch,
    RawMatch,
    SearchStats,
    smart_search,
)
from tools.cq.search.timeout import search_async_with_timeout, search_sync_with_timeout

__all__ = [
    "AUDIT",
    "DEFAULT",
    "INTERACTIVE",
    "LITERAL",
    "SMART_SEARCH_LIMITS",
    "EnrichedMatch",
    "MatchCategory",
    "QueryMode",
    "RawMatch",
    "SearchConfig",
    "SearchLimits",
    "SearchStats",
    "classify_from_node",
    "classify_from_records",
    "classify_heuristic",
    "detect_query_mode",
    "enrich_with_symtable",
    "find_call_candidates",
    "find_callers",
    "find_files_with_pattern",
    "search_async_with_timeout",
    "search_content",
    "search_sync_with_timeout",
    "smart_search",
]
