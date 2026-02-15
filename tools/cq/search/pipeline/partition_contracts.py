"""Contracts for smart-search partition execution."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct
from tools.cq.query.language import QueryLanguage


class SearchPartitionPlanV1(CqStruct, frozen=True):
    """Serializable envelope for one language partition search execution."""

    root: str
    language: QueryLanguage
    query: str
    mode: str
    include_strings: bool = False
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    max_total_matches: int = 0
    run_id: str | None = None


__all__ = ["SearchPartitionPlanV1"]
