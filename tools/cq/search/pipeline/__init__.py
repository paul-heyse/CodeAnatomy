"""Smart-search pipeline package facade."""

from __future__ import annotations

from tools.cq.search.pipeline.assembly import SearchResultAssembly, assemble_result
from tools.cq.search.pipeline.legacy import SearchPipeline
from tools.cq.search.pipeline.models import SmartSearchContext as SearchContext
from tools.cq.search.pipeline.smart_search import run_smart_search_pipeline

__all__ = [
    "SearchContext",
    "SearchPipeline",
    "SearchResultAssembly",
    "assemble_result",
    "run_smart_search_pipeline",
]
