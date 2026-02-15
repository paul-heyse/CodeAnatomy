"""Smart-search pipeline package facade with lazy exports."""

from __future__ import annotations

from typing import Any

SearchContext: Any = None
SearchPipeline: Any = None
SearchResultAssembly: Any = None
assemble_result: Any = None
run_smart_search_pipeline: Any = None

__all__ = [
    "SearchContext",
    "SearchPipeline",
    "SearchResultAssembly",
    "assemble_result",
    "run_smart_search_pipeline",
]


def __getattr__(name: str) -> Any:
    if name in {"SearchResultAssembly", "assemble_result"}:
        from tools.cq.search.pipeline.orchestration import assemble_result
        from tools.cq.search.pipeline.smart_search import SearchResultAssembly

        return SearchResultAssembly if name == "SearchResultAssembly" else assemble_result
    if name == "SearchPipeline":
        from tools.cq.search.pipeline.orchestration import SearchPipeline

        return SearchPipeline
    if name == "SearchContext":
        from tools.cq.search.pipeline.contracts import SmartSearchContext

        return SmartSearchContext
    if name == "run_smart_search_pipeline":
        from tools.cq.search.pipeline.smart_search import run_smart_search_pipeline

        return run_smart_search_pipeline
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
