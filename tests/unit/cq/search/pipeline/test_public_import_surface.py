"""Tests for smart-search pipeline public import surface."""

from __future__ import annotations

from tools.cq.search import pipeline
from tools.cq.search.pipeline.contracts import SearchConfig


def test_pipeline_exports_are_eager_and_canonical() -> None:
    """Pipeline module exports canonical public symbols eagerly."""
    assert pipeline.SearchPipelineContext is SearchConfig
    assert hasattr(pipeline, "SearchPipeline")
    assert hasattr(pipeline, "SearchResultAssembly")
    assert hasattr(pipeline, "assemble_result")


def test_pipeline_module_has_no_lazy_getattr() -> None:
    """Pipeline module does not expose lazy ``__getattr__`` machinery."""
    assert not hasattr(pipeline, "__getattr__")
