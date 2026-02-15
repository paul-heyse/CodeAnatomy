"""Result assembly bridge between smart-search and legacy pipeline scaffolding."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from tools.cq.core.schema import CqResult
from tools.cq.search.pipeline.legacy import SearchPipeline
from tools.cq.search.pipeline.models import SmartSearchContext
from tools.cq.search.pipeline.smart_search import SearchResultAssembly


def assemble_result(assembly: SearchResultAssembly) -> CqResult:
    """Assemble final CQ result from precomputed partition output."""
    from tools.cq.search.pipeline.smart_search import _assemble_smart_search_result

    pipeline = SearchPipeline(assembly.context)
    assembler = cast(
        "Callable[[SmartSearchContext, list[object]], CqResult]",
        _assemble_smart_search_result,
    )
    return pipeline.assemble(assembly.partition_results, assembler)


__all__ = ["SearchPipeline", "SearchResultAssembly", "assemble_result"]
