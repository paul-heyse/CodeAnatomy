"""Runtime context assembly for Smart Search execution phases."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.core.cache.backend_core import get_cq_cache_backend
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.python.analysis_session import get_python_analysis_session


@dataclass(frozen=True)
class SearchRuntimeContext:
    """Shared runtime services used across Smart Search phases."""

    cache_backend: CqCacheBackend
    classifier_cache: ClassifierCacheContext
    python_analysis_session: object


def build_search_runtime_context(config: SearchConfig) -> SearchRuntimeContext:
    """Build runtime context for a Smart Search request.

    Returns:
        SearchRuntimeContext: Prepared runtime context bundle.
    """
    return SearchRuntimeContext(
        cache_backend=get_cq_cache_backend(root=config.root.resolve()),
        classifier_cache=ClassifierCacheContext(),
        python_analysis_session=get_python_analysis_session,
    )


__all__ = ["SearchRuntimeContext", "build_search_runtime_context"]
