"""Tests for pipeline assembly wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.search.pipeline import orchestration as assembly_module
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.smart_search import SearchResultAssembly


def test_assemble_result_delegates_to_legacy_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test assemble result delegates to legacy pipeline."""
    sentinel = {"ok": True}
    context = cast("SearchConfig", object())

    class _Pipeline:
        def __init__(self, context: object) -> None:
            self.context = context

        def assemble(self, partition_results: object, assembler: object) -> Any:
            assert self.context is context
            assert partition_results == ["partition"]
            _ = assembler
            return sentinel

    monkeypatch.setattr(assembly_module, "SearchPipeline", _Pipeline)
    result = assembly_module.assemble_result(
        SearchResultAssembly(
            context=context,
            partition_results=cast("Any", ["partition"]),
        )
    )
    assert result == sentinel
