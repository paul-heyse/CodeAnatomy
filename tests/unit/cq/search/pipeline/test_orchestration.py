"""Tests for pipeline orchestration module (merged from assembly + section_builder + legacy)."""

from __future__ import annotations

from typing import Any, cast

import pytest
from tools.cq.core.schema import CqResult, Finding, RunMeta, Section
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.orchestration import (
    SearchPipeline,
    insert_neighborhood_preview,
    insert_target_candidates,
)

SECTIONS_WITH_CANDIDATES = 2
SECTIONS_WITH_PREVIEW = 3


class TestInsertTargetCandidates:
    """Tests for insert_target_candidates."""

    @staticmethod
    def test_inserts_at_top_when_candidates_exist() -> None:
        """Verify section is inserted at position 0."""
        sections: list[Section] = [Section(title="Existing", findings=())]
        candidates = [Finding(category="definition", message="candidate")]
        insert_target_candidates(sections, candidates=candidates)
        assert sections[0].title == "Target Candidates"
        assert len(sections) == SECTIONS_WITH_CANDIDATES

    @staticmethod
    def test_noop_when_no_candidates() -> None:
        """Verify no insertion when candidates list is empty."""
        sections: list[Section] = [Section(title="Existing", findings=())]
        insert_target_candidates(sections, candidates=[])
        assert len(sections) == 1


class TestInsertNeighborhoodPreview:
    """Tests for insert_neighborhood_preview."""

    @staticmethod
    def test_inserts_after_candidates() -> None:
        """Verify insertion at index 1 when target candidates present."""
        sections: list[Section] = [
            Section(title="Target Candidates", findings=()),
            Section(title="Other", findings=()),
        ]
        insert_neighborhood_preview(
            sections,
            findings=(Finding(category="neighborhood", message="nbr"),),
            has_target_candidates=True,
        )
        assert sections[1].title == "Neighborhood Preview"
        assert len(sections) == SECTIONS_WITH_PREVIEW

    @staticmethod
    def test_inserts_at_top_when_no_candidates() -> None:
        """Verify insertion at index 0 when no target candidates."""
        sections: list[Section] = [Section(title="Other", findings=())]
        insert_neighborhood_preview(
            sections,
            findings=(Finding(category="neighborhood", message="nbr"),),
            has_target_candidates=False,
        )
        assert sections[0].title == "Neighborhood Preview"

    @staticmethod
    def test_noop_when_no_findings() -> None:
        """Verify no insertion when findings list is empty."""
        sections: list[Section] = [Section(title="Other", findings=())]
        insert_neighborhood_preview(
            sections,
            findings=(),
            has_target_candidates=False,
        )
        assert len(sections) == 1


class TestSearchPipeline:
    """Tests for SearchPipeline facade."""

    @staticmethod
    def test_execute_runs_partition_then_assembly() -> None:
        """Verify execute composes run_partitions and assemble."""
        context = cast("SearchConfig", object())
        pipeline = SearchPipeline(context=context)
        calls: list[str] = []
        expected_result = CqResult(
            run=RunMeta(
                macro="search",
                argv=[],
                root=".",
                started_ms=0.0,
                elapsed_ms=0.0,
            )
        )

        def runner(ctx: SearchConfig) -> list[str]:
            calls.append("run")
            assert ctx is context
            return ["partition"]

        def assembler(ctx: SearchConfig, results: list[str]) -> CqResult:
            calls.append("assemble")
            assert ctx is context
            assert results == ["partition"]
            return expected_result

        result = pipeline.execute(runner, assembler)
        assert result is expected_result
        assert calls == ["run", "assemble"]


class TestAssembleResult:
    """Tests for assemble_result bridge function."""

    @staticmethod
    def test_delegates_to_legacy_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify assemble_result wires through SearchPipeline."""
        from tools.cq.search.pipeline import orchestration as orch_module
        from tools.cq.search.pipeline.smart_search import SearchResultAssembly

        sentinel = {"ok": True}
        context = cast("SearchConfig", object())

        class _Pipeline:
            def __init__(self, ctx: object) -> None:
                self.context = ctx

            def assemble(self, partition_results: object, assembler: object) -> Any:
                assert self.context is context
                assert partition_results == ["partition"]
                _ = assembler
                return sentinel

        monkeypatch.setattr(orch_module, "SearchPipeline", _Pipeline)
        result = orch_module.assemble_result(
            SearchResultAssembly(
                context=context,
                partition_results=cast("Any", ["partition"]),
            )
        )
        assert result == sentinel
