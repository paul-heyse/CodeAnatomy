"""Smart-search pipeline package facade with lazy exports."""

from __future__ import annotations

from typing import Any

SearchContext: Any = None
SearchPipeline: Any = None
SearchResultAssembly: Any = None
assemble_result: Any = None

__all__ = [
    "SearchContext",
    "SearchPipeline",
    "SearchResultAssembly",
    "assemble_result",
]


def __getattr__(name: str) -> Any:
    if name in {"SearchResultAssembly", "assemble_result"}:
        from tools.cq.search.pipeline.orchestration import assemble_result
        from tools.cq.search.pipeline.smart_search_types import SearchResultAssembly

        return SearchResultAssembly if name == "SearchResultAssembly" else assemble_result
    if name == "SearchPipeline":
        from tools.cq.search.pipeline.orchestration import SearchPipeline

        return SearchPipeline
    if name == "SearchContext":
        from tools.cq.search.pipeline.contracts import SearchConfig

        return SearchConfig
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
