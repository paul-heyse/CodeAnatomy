"""Tests for search runtime-context assembly."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search._shared.types import QueryMode, SearchLimits
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.runtime_context import build_search_runtime_context


def test_build_search_runtime_context_uses_cache_and_analysis_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime context builder wires cache backend and analysis-session provider."""
    cache_backend = object()
    analysis_session = object()

    def _fake_get_cache_backend(*, root: Path) -> object:
        _ = root
        return cache_backend

    monkeypatch.setattr(
        "tools.cq.search.pipeline.runtime_context.get_cq_cache_backend",
        _fake_get_cache_backend,
    )
    monkeypatch.setattr(
        "tools.cq.search.pipeline.runtime_context.get_python_analysis_session",
        analysis_session,
    )
    config = SearchConfig(
        root=Path(),
        query="target",
        mode=QueryMode.IDENTIFIER,
        limits=SearchLimits(),
    )

    runtime = build_search_runtime_context(config)

    assert runtime.cache_backend is cache_backend
    assert runtime.python_analysis_session is analysis_session
    assert isinstance(runtime.classifier_cache, ClassifierCacheContext)
