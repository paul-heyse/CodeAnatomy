"""Tests for smart search pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from tools.cq.core.locations import SourceSpan
from tools.cq.core.summary_contract import SemanticTelemetryV1
from tools.cq.search.pipeline.classification import classify_match
from tools.cq.search.pipeline.classifier import QueryMode, clear_caches
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
from tools.cq.search.pipeline.context_window import ContextWindow
from tools.cq.search.pipeline.contracts import SearchConfig
from tools.cq.search.pipeline.enrichment_contracts import (
    PythonEnrichmentV1,
    python_semantic_enrichment_payload,
)
from tools.cq.search.pipeline.python_semantic import (
    _python_semantic_no_signal_diagnostic,
    attach_python_semantic_enrichment,
)
from tools.cq.search.pipeline.smart_search import (
    SMART_SEARCH_LIMITS,
    _run_single_partition,
    build_candidate_searcher,
    build_finding,
    build_followups,
    build_sections,
    build_summary,
    compute_relevance_score,
    smart_search,
)
from tools.cq.search.pipeline.smart_search_types import (
    EnrichedMatch,
    LanguageSearchResult,
    RawMatch,
    SearchStats,
    SearchSummaryInputs,
    _PythonSemanticPrefetchResult,
)
from tools.cq.search.pipeline.worker_policy import resolve_search_worker_count
from tools.cq.search.semantic.models import LanguageSemanticEnrichmentOutcome
from tools.cq.search.tree_sitter.rust_lane.runtime import is_tree_sitter_rust_available

DEFAULT_LINE_NUMBER = 10
DEFAULT_COLUMN_OFFSET = 9
BASIC_SCANNED_FILES = 100
BASIC_MATCHED_FILES = 10
DEFAULT_CONFIDENCE = 0.95
EXPECTED_OCCURRENCES = 3
WORKER_INPUT_FILE_COUNT = 8
EXPECTED_WORKER_COUNT = 4
MAX_CONTEXT_END_LINE = 40


def _span(
    file: str,
    line: int,
    col: int = 0,
    end_col: int | None = None,
) -> SourceSpan:
    return SourceSpan(
        file=file,
        start_line=line,
        start_col=col,
        end_line=line,
        end_col=end_col,
    )


def _summary_inputs(
    *,
    query: str,
    stats: SearchStats,
    matches: list[EnrichedMatch],
) -> SearchSummaryInputs:
    config = SearchConfig(
        root=Path(),
        query=query,
        mode=QueryMode.IDENTIFIER,
        lang_scope="auto",
        mode_requested=QueryMode.IDENTIFIER,
        mode_chain=(QueryMode.IDENTIFIER,),
        fallback_applied=False,
        limits=SMART_SEARCH_LIMITS,
        include_globs=None,
        exclude_globs=None,
        include_strings=False,
        with_neighborhood=False,
        argv=[],
        tc=None,
        started_ms=0.0,
    )
    return SearchSummaryInputs(
        config=config,
        stats=stats,
        matches=matches,
        languages=("python", "rust"),
        language_stats={"python": stats, "rust": stats},
    )


@pytest.fixture
def sample_repo(tmp_path: Path) -> Path:
    """Create a temporary directory with sample Python files.

    Returns:
    -------
    Path
        Path to the sample repository root.
    """
    repo = tmp_path / "repo"
    repo.mkdir()

    # Create src directory structure
    src = repo / "src"
    src.mkdir()
    (src / "__init__.py").write_text("")

    (src / "module_a.py").write_text(
        """\
def build_graph(data: list) -> dict:
    '''Build a graph from data.'''
    return {"graph": data}

def process_graph(graph: dict) -> list:
    '''Process a graph.'''
    return list(graph.values())

class GraphBuilder:
    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def build_graph(self):
        '''Build the graph from nodes.'''
        return {"nodes": self.nodes}
"""
    )

    # Create utils subdirectory
    utils = src / "utils"
    utils.mkdir()
    (utils / "__init__.py").write_text("")

    (utils / "helpers.py").write_text(
        """\
from ..module_a import build_graph, GraphBuilder

def helper_function():
    '''Helper function that calls build_graph.'''
    data = [1, 2, 3]
    result = build_graph(data)  # Call to build_graph
    return result

def another_helper():
    builder = GraphBuilder()
    builder.add_node("a")
    return builder.build_graph()
"""
    )

    # Create tests directory
    tests = repo / "tests"
    tests.mkdir()
    (tests / "__init__.py").write_text("")

    (tests / "test_graph.py").write_text(
        """\
import pytest
from src.module_a import build_graph

def test_build_graph():
    '''Test build_graph function.'''
    result = build_graph([1, 2, 3])
    assert "graph" in result
"""
    )

    return repo


class TestRawMatch:
    """Tests for RawMatch struct."""

    @staticmethod
    def test_raw_match_creation() -> None:
        """Test RawMatch creation."""
        raw = RawMatch(
            span=_span("src/module.py", 10, 9, 20),
            text="result = build_graph(data)",
            match_text="build_graph",
            match_start=9,
            match_end=20,
            match_byte_start=9,
            match_byte_end=20,
        )
        assert raw.file == "src/module.py"
        assert raw.line == DEFAULT_LINE_NUMBER
        assert raw.col == DEFAULT_COLUMN_OFFSET
        assert raw.match_text == "build_graph"

    @staticmethod
    def test_raw_match_no_context_fields() -> None:
        """RawMatch should not store line-context payloads."""
        raw = RawMatch(
            span=_span("src/module.py", 10, 9, 20),
            text="result = build_graph(data)",
            match_text="build_graph",
            match_start=9,
            match_end=20,
            match_byte_start=9,
            match_byte_end=20,
        )
        assert not hasattr(raw, "context_before")
        assert not hasattr(raw, "context_after")


class TestSearchStats:
    """Tests for SearchStats struct."""

    @staticmethod
    def test_search_stats_basic() -> None:
        """Test basic SearchStats."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=BASIC_MATCHED_FILES,
            total_matches=50,
        )
        assert stats.scanned_files == BASIC_SCANNED_FILES
        assert stats.matched_files == BASIC_MATCHED_FILES
        assert stats.truncated is False
        assert stats.timed_out is False
        assert stats.max_files_hit is False
        assert stats.max_matches_hit is False

    @staticmethod
    def test_search_stats_truncated() -> None:
        """Test truncated SearchStats."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=BASIC_MATCHED_FILES,
            total_matches=500,
            truncated=True,
            max_matches_hit=True,
        )
        assert stats.truncated is True
        assert stats.max_matches_hit is True


class TestEnrichedMatch:
    """Tests for EnrichedMatch struct."""

    @staticmethod
    def test_enriched_match_creation() -> None:
        """Test EnrichedMatch creation."""
        match = EnrichedMatch(
            span=_span("src/module.py", 10, 4),
            text="result = build_graph(data)",
            match_text="build_graph",
            category="callsite",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        assert match.category == "callsite"
        assert match.confidence == DEFAULT_CONFIDENCE
        assert match.evidence_kind == "resolved_ast"


class TestRelevanceScoring:
    """Tests for relevance scoring."""

    @staticmethod
    def test_definition_scores_highest() -> None:
        """Definitions should outrank callsites in relevance scoring."""
        definition = EnrichedMatch(
            span=_span("src/module.py", 1, 0),
            text="def build_graph():",
            match_text="build_graph",
            category="definition",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        callsite = EnrichedMatch(
            span=_span("src/module.py", 10, 0),
            text="build_graph()",
            match_text="build_graph",
            category="callsite",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        assert compute_relevance_score(definition) > compute_relevance_score(callsite)

    @staticmethod
    def test_src_files_score_higher_than_tests() -> None:
        """Test that src files score higher than tests."""
        src_match = EnrichedMatch(
            span=_span("src/module.py", 1, 0),
            text="def build_graph():",
            match_text="build_graph",
            category="definition",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        test_match = EnrichedMatch(
            span=_span("tests/test_module.py", 1, 0),
            text="def test_build_graph():",
            match_text="build_graph",
            category="definition",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        assert compute_relevance_score(src_match) > compute_relevance_score(test_match)

    @staticmethod
    def test_comment_match_scores_low() -> None:
        """Test that comment matches score low."""
        comment = EnrichedMatch(
            span=_span("src/module.py", 1, 0),
            text="# build_graph is important",
            match_text="build_graph",
            category="comment_match",
            confidence=0.95,
            evidence_kind="heuristic",
        )
        definition = EnrichedMatch(
            span=_span("src/module.py", 1, 0),
            text="def build_graph():",
            match_text="build_graph",
            category="definition",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        assert compute_relevance_score(comment) < compute_relevance_score(definition)


class TestClassifyMatch:
    """Tests for match classification."""

    @staticmethod
    def test_classify_comment(sample_repo: Path) -> None:
        """Test classification of comment match."""
        clear_caches()
        raw = RawMatch(
            span=_span("src/module_a.py", 1, 2, 13),
            text="# build_graph is here",
            match_text="build_graph",
            match_start=2,
            match_end=13,
            match_byte_start=2,
            match_byte_end=13,
        )
        # Create file with comment
        (sample_repo / "src" / "module_a.py").write_text("# build_graph is here\n")
        enriched = classify_match(raw, sample_repo, cache_context=ClassifierCacheContext())
        assert enriched.category == "comment_match"
        assert enriched.evidence_kind == "heuristic"

    @staticmethod
    def test_classify_import(sample_repo: Path) -> None:
        """Test classification of import match."""
        clear_caches()
        raw = RawMatch(
            span=_span("src/module_a.py", 1, 7, 18),
            text="import build_graph",
            match_text="build_graph",
            match_start=7,
            match_end=18,
            match_byte_start=7,
            match_byte_end=18,
        )
        # Create file with import
        (sample_repo / "src" / "module_a.py").write_text("import build_graph\n")
        enriched = classify_match(raw, sample_repo, cache_context=ClassifierCacheContext())
        assert enriched.category == "import"

    @staticmethod
    def test_classify_import_force_semantic_enrichment(sample_repo: Path) -> None:
        """Forced semantic enrichment should attach Python enrichment for imports."""
        clear_caches()
        raw = RawMatch(
            span=_span("src/module_a.py", 1, 7, 18),
            text="import build_graph",
            match_text="build_graph",
            match_start=7,
            match_end=18,
            match_byte_start=7,
            match_byte_end=18,
        )
        (sample_repo / "src" / "module_a.py").write_text("import build_graph\n", encoding="utf-8")
        enriched = classify_match(
            raw,
            sample_repo,
            cache_context=ClassifierCacheContext(),
            force_semantic_enrichment=True,
        )
        assert enriched.category == "import"
        assert isinstance(enriched.python_enrichment, PythonEnrichmentV1)


class TestBuildFinding:
    """Tests for Finding construction."""

    @staticmethod
    def test_build_finding_basic(sample_repo: Path) -> None:
        """Test basic Finding construction."""
        match = EnrichedMatch(
            span=_span("src/module.py", 10, 4),
            text="result = build_graph(data)",
            match_text="build_graph",
            category="callsite",
            confidence=0.95,
            evidence_kind="resolved_ast",
        )
        finding = build_finding(match, sample_repo)
        assert finding.category == "callsite"
        assert finding.anchor is not None
        assert finding.anchor.file == "src/module.py"
        assert finding.anchor.line == DEFAULT_LINE_NUMBER

    @staticmethod
    def test_build_finding_with_scope(sample_repo: Path) -> None:
        """Test Finding with containing scope."""
        match = EnrichedMatch(
            span=_span("src/module.py", 10, 4),
            text="result = build_graph(data)",
            match_text="build_graph",
            category="callsite",
            confidence=0.95,
            evidence_kind="resolved_ast",
            containing_scope="helper_function",
        )
        finding = build_finding(match, sample_repo)
        assert "helper_function" in finding.message


class TestBuildFollowups:
    """Tests for follow-up suggestions."""

    @staticmethod
    def test_followups_for_identifier_with_defs() -> None:
        """Test follow-ups when definitions found."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 1, 0),
                text="def build_graph():",
                match_text="build_graph",
                category="definition",
                confidence=0.95,
                evidence_kind="resolved_ast",
            )
        ]
        followups = build_followups(matches, "build_graph", QueryMode.IDENTIFIER)
        assert len(followups) > 0
        # Should suggest finding callers
        messages = [f.message for f in followups]
        assert any("callers" in m.lower() for m in messages)

    @staticmethod
    def test_followups_for_identifier_with_calls() -> None:
        """Test follow-ups when callsites found."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 10, 0),
                text="build_graph()",
                match_text="build_graph",
                category="callsite",
                confidence=0.95,
                evidence_kind="resolved_ast",
            )
        ]
        followups = build_followups(matches, "build_graph", QueryMode.IDENTIFIER)
        # Should suggest impact analysis
        messages = [f.message for f in followups]
        assert any("impact" in m.lower() for m in messages)

    @staticmethod
    def test_no_followups_for_regex() -> None:
        """Test no follow-ups for regex mode."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 1, 0),
                text="def build_graph():",
                match_text="build_graph",
                category="definition",
                confidence=0.95,
                evidence_kind="resolved_ast",
            )
        ]
        followups = build_followups(matches, "build.*", QueryMode.REGEX)
        # Regex mode shouldn't generate call-based followups
        assert len(followups) == 0


class TestBuildSummary:
    """Tests for summary construction."""

    @staticmethod
    def test_summary_basic() -> None:
        """Test basic summary construction."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=10,
            total_matches=50,
        )
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 1, 0),
                text="def build_graph():",
                match_text="build_graph",
                category="definition",
                confidence=0.95,
                evidence_kind="resolved_ast",
            )
        ]
        summary = build_summary(_summary_inputs(query="build_graph", stats=stats, matches=matches))
        assert summary["query"] == "build_graph"
        assert summary["mode"] == "identifier"
        assert summary["scanned_files"] == BASIC_SCANNED_FILES
        assert summary["scanned_files_is_estimate"] is True
        assert summary["matched_files"] == BASIC_MATCHED_FILES
        assert summary["returned_matches"] == 1

    @staticmethod
    def test_summary_with_truncation() -> None:
        """Test summary with truncation."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=10,
            total_matches=500,
            truncated=True,
            max_matches_hit=True,
        )
        summary = build_summary(_summary_inputs(query="build_graph", stats=stats, matches=[]))
        assert summary["truncated"] is True
        assert summary["caps_hit"] == "max_total_matches"

    @staticmethod
    def test_summary_multilang_contract_keys() -> None:
        """Summary should expose canonical multilang contract keys."""
        stats = SearchStats(
            scanned_files=1,
            matched_files=1,
            total_matches=1,
        )
        summary = build_summary(_summary_inputs(query="build_graph", stats=stats, matches=[]))
        assert summary["lang_scope"] == "auto"
        assert summary["language_order"] == ["python", "rust"]
        assert isinstance(summary["languages"], dict)
        assert isinstance(summary["cross_language_diagnostics"], list)
        assert isinstance(summary["language_capabilities"], dict)


class TestBuildSections:
    """Tests for section construction."""

    @staticmethod
    def test_sections_include_resolved_objects(sample_repo: Path) -> None:
        """Object-resolved output should include a deduplicated object section."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 1, 0),
                text="def build_graph():",
                match_text="build_graph",
                category="definition",
                confidence=0.95,
                evidence_kind="resolved_ast",
            )
        ]
        sections = build_sections(matches, sample_repo, "build_graph", QueryMode.IDENTIFIER)
        titles = [s.title for s in sections]
        assert "Resolved Objects" in titles

    @staticmethod
    def test_sections_include_occurrences_for_identifier(sample_repo: Path) -> None:
        """Identifier mode should include explicit occurrence rows."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 1, 0),
                text="def build_graph():",
                match_text="build_graph",
                category="definition",
                confidence=0.95,
                evidence_kind="resolved_ast",
            )
        ]
        sections = build_sections(matches, sample_repo, "build_graph", QueryMode.IDENTIFIER)
        titles = [s.title for s in sections]
        assert "Occurrences" in titles

    @staticmethod
    def test_non_code_matches_collapsed(sample_repo: Path) -> None:
        """Test that non-code matches section is collapsed."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 1, 0),
                text="# build_graph comment",
                match_text="build_graph",
                category="comment_match",
                confidence=0.95,
                evidence_kind="heuristic",
            )
        ]
        sections = build_sections(matches, sample_repo, "build_graph", QueryMode.IDENTIFIER)
        non_code_section = next((s for s in sections if "Non-Code" in s.title), None)
        if non_code_section:
            assert non_code_section.collapsed is True

    @staticmethod
    def test_occurrences_include_block_ranges(sample_repo: Path) -> None:
        """Occurrence rows should surface location + enclosing block range."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 10, 4),
                text="build_graph()",
                match_text="build_graph",
                category="callsite",
                confidence=0.95,
                evidence_kind="resolved_ast",
                containing_scope="helper",
            ),
            EnrichedMatch(
                span=_span("src/module.py", 11, 4),
                text="build_graph()",
                match_text="build_graph",
                category="callsite",
                confidence=0.95,
                evidence_kind="resolved_ast",
                containing_scope="helper",
            ),
            EnrichedMatch(
                span=_span("src/module.py", 20, 4),
                text="build_graph()",
                match_text="build_graph",
                category="callsite",
                confidence=0.95,
                evidence_kind="resolved_ast",
                containing_scope="worker",
            ),
        ]
        sections = build_sections(matches, sample_repo, "build_graph", QueryMode.IDENTIFIER)
        resolved_section = next(s for s in sections if s.title == "Resolved Objects")
        assert resolved_section.findings
        assert "object_id=" in resolved_section.findings[0].message
        resolved_occurrence_total = 0
        for finding in resolved_section.findings:
            resolved_occurrences = finding.details.get("occurrences")
            assert isinstance(resolved_occurrences, list)
            resolved_occurrence_total += len(resolved_occurrences)
            if resolved_occurrences:
                first_resolved_occurrence = resolved_occurrences[0]
                assert isinstance(first_resolved_occurrence, dict)
                assert "line_id" in first_resolved_occurrence
                assert isinstance(first_resolved_occurrence.get("block_ref"), str)
        assert resolved_occurrence_total == EXPECTED_OCCURRENCES

        occurrences_section = next(s for s in sections if s.title == "Occurrences")
        assert len(occurrences_section.findings) == EXPECTED_OCCURRENCES
        assert all("block" in finding.message for finding in occurrences_section.findings)
        assert all("line_id=" in finding.message for finding in occurrences_section.findings)
        assert all("object_id=" in finding.message for finding in occurrences_section.findings)

    @staticmethod
    def test_sections_include_strings_when_enabled(sample_repo: Path) -> None:
        """Non-code matches should surface in occurrence rows when include_strings is set."""
        matches = [
            EnrichedMatch(
                span=_span("src/module.py", 2, 0),
                text="# build_graph comment",
                match_text="build_graph",
                category="comment_match",
                confidence=0.95,
                evidence_kind="heuristic",
            )
        ]
        sections = build_sections(matches, sample_repo, "build_graph", QueryMode.IDENTIFIER)
        occurrences_section = next(s for s in sections if s.title == "Occurrences")
        assert occurrences_section.findings == []
        sections_with_strings = build_sections(
            matches,
            sample_repo,
            "build_graph",
            QueryMode.IDENTIFIER,
            include_strings=True,
        )
        with_strings_occurrences = next(
            s for s in sections_with_strings if s.title == "Occurrences"
        )
        assert len(with_strings_occurrences.findings) == 1


class TestSmartSearch:
    """Tests for the full smart search pipeline."""

    @staticmethod
    def test_smart_search_identifier(sample_repo: Path) -> None:
        """Test smart search with identifier mode."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        assert result.run.macro == "search"
        assert "query" in result.summary
        assert result.summary["mode"] == "identifier"
        assert isinstance(result.summary.get("enrichment_telemetry"), dict)
        # Should find matches
        assert len(result.evidence) > 0

    @staticmethod
    def test_python_enrichment_telemetry_uses_python_resolution_stage(sample_repo: Path) -> None:
        """Telemetry stage buckets should use python_resolution and exclude legacy libcst."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph", lang_scope="python")
        telemetry = result.summary.get("enrichment_telemetry")
        assert isinstance(telemetry, dict)
        python_bucket = telemetry.get("python")
        assert isinstance(python_bucket, dict)
        stages = python_bucket.get("stages")
        timings = python_bucket.get("timings_ms")
        assert isinstance(stages, dict)
        assert isinstance(timings, dict)
        assert "python_resolution" in stages
        assert "python_resolution" in timings
        assert "libcst" not in stages
        assert "libcst" not in timings

    @staticmethod
    def test_smart_search_scanned_files_exact(sample_repo: Path) -> None:
        """Smart search should report exact scanned file counts when available."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        summary = cast("Mapping[str, object]", result.summary)
        assert summary["scanned_files_is_estimate"] is False
        scanned_files = cast("int", summary["scanned_files"])
        matched_files = cast("int", summary["matched_files"])
        assert scanned_files >= matched_files

    @staticmethod
    def test_smart_search_classification_uses_fixed_worker_cap(
        tmp_path: Path,
    ) -> None:
        """Classification path should use at most four worker processes."""
        for idx in range(WORKER_INPUT_FILE_COUNT):
            (tmp_path / f"mod_{idx}.py").write_text(
                "def build_graph():\n    return 1\n",
                encoding="utf-8",
            )
        clear_caches()
        result = smart_search(tmp_path, "build_graph", lang_scope="python")
        assert result.evidence
        assert resolve_search_worker_count(WORKER_INPUT_FILE_COUNT) == EXPECTED_WORKER_COUNT

    @staticmethod
    def test_smart_search_with_include_globs(sample_repo: Path) -> None:
        """Test smart search with include globs."""
        clear_caches()
        result = smart_search(
            sample_repo,
            "build_graph",
            include_globs=["src/**"],
        )
        # All matches should be in src
        for finding in result.evidence:
            if finding.anchor:
                assert finding.anchor.file.startswith("src/") or "src" in finding.anchor.file

    @staticmethod
    def test_smart_search_with_tools_scope_glob(tmp_path: Path) -> None:
        """Directory include globs should match files under tools/ recursively."""
        target = tmp_path / "tools" / "cq" / "search"
        target.mkdir(parents=True)
        (target / "python_analysis_session.py").write_text(
            "class PythonAnalysisSession:\n    pass\n",
            encoding="utf-8",
        )
        (tmp_path / "src").mkdir(parents=True)
        (tmp_path / "src" / "python_analysis_session.py").write_text(
            "class PythonAnalysisSession:\n    pass\n",
            encoding="utf-8",
        )

        clear_caches()
        result = smart_search(
            tmp_path,
            "PythonAnalysisSession",
            lang_scope="python",
            include_globs=["tools/**"],
        )
        assert result.evidence
        for finding in result.evidence:
            if finding.anchor:
                assert finding.anchor.file.removeprefix("./").startswith("tools/")

    @staticmethod
    def test_smart_search_with_rust_scope_glob(tmp_path: Path) -> None:
        """Rust directory include globs should match Rust files recursively."""
        rust_src = tmp_path / "rust" / "codeanatomy_engine_py" / "src"
        rust_src.mkdir(parents=True)
        (rust_src / "compiler.rs").write_text(
            "pub fn compile_target() -> i32 { 1 }\n",
            encoding="utf-8",
        )
        (tmp_path / "src").mkdir(parents=True)
        (tmp_path / "src" / "compiler.py").write_text(
            "def compile_target() -> int:\n    return 1\n",
            encoding="utf-8",
        )

        clear_caches()
        result = smart_search(
            tmp_path,
            "compile_target",
            lang_scope="rust",
            include_globs=["rust/**"],
        )
        assert result.evidence
        for finding in result.evidence:
            if finding.anchor:
                assert finding.anchor.file.removeprefix("./").startswith("rust/")

    @staticmethod
    def test_smart_search_with_tools_cq_scope_glob(tmp_path: Path) -> None:
        """Nested directory include globs should remain reliable for tools/cq."""
        target = tmp_path / "tools" / "cq" / "search"
        target.mkdir(parents=True)
        (target / "python_analysis_session.py").write_text(
            "class PythonAnalysisSession:\n    pass\n",
            encoding="utf-8",
        )

        clear_caches()
        result = smart_search(
            tmp_path,
            "PythonAnalysisSession",
            lang_scope="python",
            include_globs=["tools/cq/**"],
        )
        assert result.evidence
        for finding in result.evidence:
            if finding.anchor:
                assert finding.anchor.file.removeprefix("./").startswith("tools/cq/")

    @staticmethod
    def test_python_query_under_rust_include_glob_returns_no_evidence(tmp_path: Path) -> None:
        """Python searches constrained to rust/** should remain empty."""
        rust_src = tmp_path / "rust" / "codeanatomy_engine_py" / "src"
        rust_src.mkdir(parents=True)
        (rust_src / "compiler.rs").write_text(
            "pub fn compile_target() -> i32 { 1 }\n",
            encoding="utf-8",
        )

        clear_caches()
        result = smart_search(
            tmp_path,
            "compile_target",
            lang_scope="python",
            include_globs=["rust/**"],
        )
        assert not result.evidence

    @staticmethod
    def test_rust_query_under_python_include_glob_returns_no_evidence(tmp_path: Path) -> None:
        """Rust searches constrained to tools/** with only Python files should remain empty."""
        target = tmp_path / "tools" / "cq" / "search"
        target.mkdir(parents=True)
        (target / "python_analysis_session.py").write_text(
            "class PythonAnalysisSession:\n    pass\n",
            encoding="utf-8",
        )

        clear_caches()
        result = smart_search(
            tmp_path,
            "PythonAnalysisSession",
            lang_scope="rust",
            include_globs=["tools/**"],
        )
        assert not result.evidence

    @staticmethod
    def test_smart_search_sections_present(sample_repo: Path) -> None:
        """Test that sections are present in result."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        assert len(result.sections) > 0
        titles = [section.title for section in result.sections]
        assert "Resolved Objects" in titles
        assert "Occurrences" in titles
        assert "Target Candidates" in titles

    @staticmethod
    def test_smart_search_key_findings(sample_repo: Path) -> None:
        """Test that key findings are populated."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        assert len(result.key_findings) > 0 or len(result.sections) == 0
        first = result.key_findings[0]
        assert first.category in {
            "definition",
            "resolved_object",
            "from_import",
            "import",
            "callsite",
        }

    @staticmethod
    def test_search_insight_target_grounded_from_definitions(sample_repo: Path) -> None:
        """Insight target should resolve to a definition location when available."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        insight = cast("dict[str, object]", result.summary.get("front_door_insight", {}))
        assert insight
        target = cast("dict[str, object]", insight.get("target", {}))
        assert target.get("kind") != "query"
        location = cast("dict[str, object]", target.get("location", {}))
        assert isinstance(location.get("file"), str)
        location_file = str(location.get("file"))
        assert location_file.removeprefix("./").startswith("src/")
        assert isinstance(location.get("line"), int)
        assert target.get("kind") in {"function", "class", "type"}

    @staticmethod
    def test_search_insight_excludes_annotation_reference_target_kinds(tmp_path: Path) -> None:
        """Definition candidates should not surface annotation/reference target kinds."""
        module = tmp_path / "module.py"
        module.write_text(
            "\n".join(
                [
                    "class AsyncService:",
                    "    pass",
                    "",
                    "def build_pipeline() -> tuple[AsyncService, int]:",
                    "    return (AsyncService(), 1)",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        clear_caches()
        result = smart_search(tmp_path, "AsyncService", lang_scope="python")
        insight = cast("dict[str, object]", result.summary.get("front_door_insight", {}))
        target = cast("dict[str, object]", insight.get("target", {}))
        assert target.get("kind") in {"function", "class", "type"}

    @staticmethod
    def test_search_neighborhood_preview_section_present_for_resolved_definition(
        sample_repo: Path,
    ) -> None:
        """Resolved definition targets should include a neighborhood preview section."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph", with_neighborhood=True)
        titles = [section.title for section in result.sections]
        assert "Neighborhood Preview" in titles

    @staticmethod
    def test_search_neighborhood_preview_disabled_by_default(sample_repo: Path) -> None:
        """Neighborhood preview should be opt-in for search latency control."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        titles = [section.title for section in result.sections]
        assert "Neighborhood Preview" not in titles

    @staticmethod
    def test_search_degradation_notes_are_deduplicated(sample_repo: Path) -> None:
        """Degradation notes should avoid duplicate status markers."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        insight = cast("dict[str, object]", result.summary.get("front_door_insight", {}))
        degradation = cast("dict[str, object]", insight.get("degradation", {}))
        notes = cast("list[str]", degradation.get("notes", []))
        assert len(notes) == len(set(notes))

    @staticmethod
    def test_evidence_cap(sample_repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Evidence should respect MAX_EVIDENCE cap."""
        from tools.cq.search.pipeline import assembly as assembly_module

        clear_caches()
        monkeypatch.setattr(assembly_module, "MAX_EVIDENCE", 1)
        result = smart_search(sample_repo, "build_graph")
        assert len(result.evidence) <= 1

    @staticmethod
    def test_multilang_order_is_python_first(tmp_path: Path) -> None:
        """Merged evidence should prefer Python before Rust for tied relevance."""
        (tmp_path / "a.py").write_text("def build_graph():\n    return 1\n", encoding="utf-8")
        (tmp_path / "b.rs").write_text("fn build_graph() -> i32 { 1 }\n", encoding="utf-8")
        clear_caches()
        result = smart_search(tmp_path, "build_graph")
        summary = cast("Mapping[str, object]", result.summary)
        assert summary["language_order"] == ["python", "rust"]
        assert result.evidence
        assert result.evidence[0].details.get("language") == "python"

    @staticmethod
    def test_rust_scope_filters_out_python_files(tmp_path: Path) -> None:
        """Rust scope should not emit findings anchored to Python files."""
        (tmp_path / "mod.py").write_text("def classify_match():\n    return 1\n", encoding="utf-8")
        (tmp_path / "lib.rs").write_text("fn classify_match() -> i32 { 1 }\n", encoding="utf-8")

        clear_caches()
        result = smart_search(tmp_path, "classify_match", lang_scope="rust")
        anchors = [finding.anchor for finding in result.evidence if finding.anchor is not None]
        assert anchors
        assert all(Path(anchor.file).suffix == ".rs" for anchor in anchors)
        assert all(finding.details.get("language") == "rust" for finding in result.evidence)


class TestSmartSearchFiltersAndEnrichment:
    """Additional smart-search tests split from TestSmartSearch."""

    @staticmethod
    def test_python_scope_filters_out_rust_files(tmp_path: Path) -> None:
        """Python scope should not emit findings anchored to Rust files."""
        (tmp_path / "mod.py").write_text("def classify_match():\n    return 1\n", encoding="utf-8")
        (tmp_path / "lib.rs").write_text("fn classify_match() -> i32 { 1 }\n", encoding="utf-8")

        clear_caches()
        result = smart_search(tmp_path, "classify_match", lang_scope="python")
        anchors = [finding.anchor for finding in result.evidence if finding.anchor is not None]
        assert anchors
        assert all(Path(anchor.file).suffix in {".py", ".pyi"} for anchor in anchors)
        assert all(finding.details.get("language") == "python" for finding in result.evidence)

    @staticmethod
    def test_top_level_import_context_window_is_compact(tmp_path: Path) -> None:
        """Top-level import matches should not expand context to the full file."""
        filler = "\n".join(f"    value_{idx} = {idx}" for idx in range(120))
        (tmp_path / "module.py").write_text(
            "\n".join(
                [
                    "import os",
                    "import sys",
                    "",
                    "CONSTANT = 1",
                    "",
                    "def worker():",
                    filler,
                    "    return value_0",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        clear_caches()
        result = smart_search(tmp_path, "os", lang_scope="python")
        assert result.evidence
        detail_data = result.evidence[0].details.data
        context_window = detail_data.get("context_window")
        assert isinstance(context_window, ContextWindow)
        start_line = context_window.start_line
        end_line = context_window.end_line
        assert start_line == 1
        assert end_line < MAX_CONTEXT_END_LINE

    @staticmethod
    def test_rust_scope_in_python_tree_has_no_scope_drop_anomaly(tmp_path: Path) -> None:
        """Rust searches constrained to Python-only trees should not inflate dropped_by_scope."""
        search_dir = tmp_path / "tools" / "cq" / "search"
        search_dir.mkdir(parents=True)
        (search_dir / "smart_search.py").write_text(
            "def classify_match():\n    return 1\n",
            encoding="utf-8",
        )

        clear_caches()
        result = smart_search(
            tmp_path,
            "classify_match",
            lang_scope="rust",
            include_globs=["tools/cq/search/**"],
        )
        dropped_by_scope = result.summary.get("dropped_by_scope")
        if isinstance(dropped_by_scope, dict):
            assert dropped_by_scope.get("rust", 0) == 0
        assert not result.evidence

    @staticmethod
    def test_auto_identifier_falls_back_to_literal_when_no_hits(tmp_path: Path) -> None:
        """Auto-identifier mode should fallback to literal search when identifier is empty."""
        (tmp_path / "module.py").write_text("build_graph_v2 = 1\n", encoding="utf-8")
        clear_caches()
        result = smart_search(tmp_path, "build_graph")
        assert result.evidence
        assert result.summary.get("mode_requested") == "auto"
        assert result.summary.get("mode_effective") == "literal"
        assert result.summary.get("mode_chain") == ["identifier", "literal"]
        assert result.summary.get("fallback_applied") is True

    @staticmethod
    def test_forced_identifier_does_not_fallback_to_literal(tmp_path: Path) -> None:
        """Forced mode should never auto-fallback."""
        (tmp_path / "module.py").write_text("build_graph_v2 = 1\n", encoding="utf-8")
        clear_caches()
        result = smart_search(tmp_path, "build_graph", mode=QueryMode.IDENTIFIER)
        assert not result.evidence
        assert result.summary.get("mode_requested") == "identifier"
        assert result.summary.get("mode_effective") == "identifier"
        assert result.summary.get("mode_chain") == ["identifier"]
        assert result.summary.get("fallback_applied") is False

    @staticmethod
    def test_file_include_scope_accepts_exact_file_path(tmp_path: Path) -> None:
        """Single-file include globs should constrain search to that exact file."""
        target = tmp_path / "tools" / "cq" / "search"
        target.mkdir(parents=True)
        wanted = target / "classifier.py"
        wanted.write_text("def classify_match():\n    return 1\n", encoding="utf-8")
        (target / "other.py").write_text("def classify_match():\n    return 2\n", encoding="utf-8")

        clear_caches()
        result = smart_search(
            tmp_path,
            "classify_match",
            lang_scope="python",
            include_globs=["tools/cq/search/classifier.py"],
        )
        assert result.evidence
        anchored_files = [
            finding.anchor.file for finding in result.evidence if finding.anchor is not None
        ]
        assert anchored_files
        assert all(file.endswith("tools/cq/search/classifier.py") for file in anchored_files)

    @staticmethod
    def test_context_snippet_keeps_header_and_anchor_block(tmp_path: Path) -> None:
        """Context snippets should preserve function top and the match anchor block."""
        filler = "\n".join(f"    filler_{i} = {i}" for i in range(40))
        (tmp_path / "module.py").write_text(
            "\n".join(
                [
                    "def outer():",
                    '    """docstring should be omitted"""',
                    "    head = 1",
                    filler,
                    "    if condition:",
                    "        value = 10",
                    "        target_marker = value",
                    "    tail = head",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        clear_caches()
        result = smart_search(tmp_path, "target_marker", lang_scope="python")
        assert result.evidence
        detail_data = result.evidence[0].details.data
        snippet = detail_data.get("context_snippet")
        assert isinstance(snippet, str)
        assert "def outer():" in snippet
        assert "if condition:" in snippet
        assert "target_marker = value" in snippet
        assert "docstring should be omitted" not in snippet

    @staticmethod
    def test_cross_language_warning_for_python_intent_rust_only(tmp_path: Path) -> None:
        """Python-oriented text with Rust-only matches should emit a warning."""
        (tmp_path / "only.rs").write_text("// decorator\nfn f() {}\n", encoding="utf-8")
        clear_caches()
        result = smart_search(tmp_path, "decorator")
        diagnostics = cast("list[object]", result.summary["cross_language_diagnostics"])
        assert diagnostics
        assert isinstance(diagnostics[0], dict)
        first = cast("dict[str, object]", diagnostics[0])
        assert first.get("code") == "ML001"
        assert "Python-oriented query produced no Python matches" in str(first.get("message"))

    @staticmethod
    @pytest.mark.skipif(
        not is_tree_sitter_rust_available(),
        reason="tree-sitter-rust is not available in this environment",
    )
    def test_rust_tree_sitter_enrichment_attached(tmp_path: Path) -> None:
        """Rust findings should include optional tree-sitter enrichment details."""
        (tmp_path / "lib.rs").write_text(
            'fn build_graph() {\n    println!("x");\n}\n',
            encoding="utf-8",
        )
        clear_caches()
        result = smart_search(tmp_path, "build_graph", lang_scope="rust")
        assert result.evidence
        detail_data = result.evidence[0].details.data if result.evidence[0].details else None
        assert isinstance(detail_data, dict)
        enrichment = detail_data.get("enrichment")
        assert isinstance(enrichment, dict)
        assert enrichment.get("language") == "rust"
        rust_payload = enrichment.get("rust")
        assert isinstance(rust_payload, dict)
        assert rust_payload.get("enrichment_status") in {"applied", "degraded", "skipped"}

    @staticmethod
    def test_python_enrichment_dictionary_attached(sample_repo: Path) -> None:
        """Python findings should expose structured enrichment payloads."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph", lang_scope="python")
        assert result.evidence
        payloads = [
            finding.details.data.get("enrichment")
            for finding in result.evidence
            if isinstance(finding.details.data.get("enrichment"), dict)
        ]
        assert payloads
        first = cast("dict[str, object]", payloads[0])
        assert first.get("language") == "python"
        assert isinstance(first.get("python"), dict)

    @staticmethod
    def test_python_semantic_summary_fields_present(sample_repo: Path) -> None:
        """Search summaries should include additive PythonSemantic metadata blocks."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph", lang_scope="python")
        summary = cast("Mapping[str, object]", result.summary)
        assert "python_semantic_overview" in summary
        assert "python_semantic_telemetry" in summary
        assert "python_semantic_diagnostics" in summary

    @staticmethod
    def test_python_semantic_payload_key_attached_when_available(sample_repo: Path) -> None:
        """Per-finding enrichment should expose a dedicated python_semantic payload key."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph", lang_scope="python")
        assert result.evidence
        first = result.evidence[0]
        enrichment = first.details.data.get("enrichment")
        assert isinstance(enrichment, dict)
        # Key is always present when PythonSemantic enrichment is materialized.
        if "python_semantic" in enrichment:
            assert isinstance(enrichment["python_semantic"], dict)

    @staticmethod
    def test_python_semantic_no_signal_diagnostic_normalizes_capability_reason() -> None:
        """Capability-specific coverage reasons should remain explicit."""
        diagnostic = _python_semantic_no_signal_diagnostic(
            ("no_python_semantic_signal",),
            coverage_reason="no_python_semantic_signal:unsupported_capability",
        )
        assert diagnostic["reason"] == "unsupported_capability"

    @staticmethod
    def test_python_semantic_no_signal_diagnostic_normalizes_request_interface_reason() -> None:
        """Request-interface failures should not be collapsed into generic no-signal."""
        diagnostic = _python_semantic_no_signal_diagnostic(
            ("no_python_semantic_signal",),
            coverage_reason="request_interface_unavailable",
        )
        assert diagnostic["reason"] == "request_interface_unavailable"

    @staticmethod
    def test_python_semantic_no_signal_diagnostic_defaults_to_no_signal() -> None:
        """Unexpected/noisy reasons should collapse to canonical no_signal."""
        diagnostic = _python_semantic_no_signal_diagnostic(("empty_payload",), coverage_reason=None)
        assert diagnostic["reason"] == "no_signal"

    @staticmethod
    def test_attach_python_semantic_uses_prefetched_payload(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Prefetched PythonSemantic payloads should bypass synchronous fallback calls."""
        clear_caches()
        ctx = SearchConfig(
            root=tmp_path,
            query="target",
            mode=QueryMode.IDENTIFIER,
            limits=SMART_SEARCH_LIMITS,
        )
        match = EnrichedMatch(
            span=_span("sample.py", 3, 4),
            text="target()",
            match_text="target",
            category="callsite",
            confidence=0.9,
            evidence_kind="resolved_ast",
            language="python",
        )
        key = ("sample.py", 3, 4, "target")
        prefetched_payload: dict[str, object] = {
            "call_graph": {"incoming_total": 1, "outgoing_total": 0}
        }
        prefetched = _PythonSemanticPrefetchResult(
            payloads={key: prefetched_payload},
            attempted_keys={key},
            telemetry={"attempted": 1, "applied": 1, "failed": 0, "skipped": 0, "timed_out": 0},
            diagnostics=[],
        )

        def _boom(*_args: object, **_kwargs: object) -> dict[str, object] | None:
            msg = "fallback python_semantic call should not run when prefetched payload exists"
            raise AssertionError(msg)

        monkeypatch.setattr(
            "tools.cq.search.pipeline.python_semantic._python_semantic_enrich_match", _boom
        )
        enriched, _overview, telemetry, diagnostics = attach_python_semantic_enrichment(
            ctx=ctx,
            matches=[match],
            prefetched=prefetched,
        )
        assert not diagnostics
        assert (
            python_semantic_enrichment_payload(enriched[0].python_semantic_enrichment)
            == prefetched_payload
        )
        telemetry_map = cast("Mapping[str, object]", telemetry)
        assert telemetry_map.get("attempted") == 1
        assert telemetry_map.get("applied") == 1

    @staticmethod
    def test_run_single_partition_delegates_to_enrichment_phase(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Single-partition helper delegates to enrichment phase orchestration."""
        clear_caches()
        ctx = SearchConfig(
            root=tmp_path,
            query="target",
            mode=QueryMode.IDENTIFIER,
            limits=SMART_SEARCH_LIMITS,
        )
        raw_match = RawMatch(
            span=_span("sample.py", 3, 4),
            text="target()",
            match_text="target",
            match_start=4,
            match_end=10,
            match_byte_start=4,
            match_byte_end=10,
        )
        captured: dict[str, object] = {}

        def _fake_enrichment_phase(
            _plan: object,
            *,
            config: SearchConfig,
            mode: QueryMode,
        ) -> LanguageSearchResult:
            captured["config"] = config
            captured["mode"] = mode
            return LanguageSearchResult(
                lang="python",
                raw_matches=[raw_match],
                stats=SearchStats(scanned_files=1, matched_files=1, total_matches=1),
                pattern=r"\btarget\b",
                enriched_matches=[],
                dropped_by_scope=0,
                python_semantic_prefetch=_PythonSemanticPrefetchResult(
                    telemetry={
                        "attempted": 1,
                        "applied": 0,
                        "failed": 0,
                        "skipped": 0,
                        "timed_out": 0,
                    }
                ),
            )

        monkeypatch.setattr(
            "tools.cq.search.pipeline.smart_search.run_enrichment_phase", _fake_enrichment_phase
        )

        result = _run_single_partition(ctx, "python", mode=QueryMode.IDENTIFIER)
        assert captured["config"] is ctx
        assert captured["mode"] == QueryMode.IDENTIFIER
        assert result.python_semantic_prefetch is not None

    @staticmethod
    def test_rust_tree_sitter_fail_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tree-sitter enrichment failures must not break core search results."""
        (tmp_path / "lib.rs").write_text(
            'fn build_graph() {\n    println!("x");\n}\n',
            encoding="utf-8",
        )
        clear_caches()
        import importlib

        smart_search_module = importlib.import_module("tools.cq.search.pipeline.smart_search")

        def _boom(*_args: object, **_kwargs: object) -> dict[str, object]:
            msg = "forced enrichment failure"
            raise RuntimeError(msg)

        monkeypatch.setattr(
            "tools.cq.search.pipeline.classification.enrich_rust_context_by_byte_range",
            _boom,
        )
        result = smart_search_module.smart_search(tmp_path, "build_graph", lang_scope="rust")
        assert result.summary["query"] == "build_graph"
        assert result.evidence


class TestCandidateSearcher:
    """Tests for candidate searcher construction."""

    @staticmethod
    def test_build_searcher_identifier(sample_repo: Path) -> None:
        """Test building searcher for identifier mode."""
        _searcher, pattern = build_candidate_searcher(
            sample_repo,
            "build_graph",
            QueryMode.IDENTIFIER,
            SMART_SEARCH_LIMITS,
        )
        assert r"\b" not in pattern  # Boundaries are handled by rg -w
        assert pattern == "build_graph"

    @staticmethod
    def test_build_searcher_literal(sample_repo: Path) -> None:
        """Test building searcher for literal mode."""
        _searcher, pattern = build_candidate_searcher(
            sample_repo,
            "hello world",
            QueryMode.LITERAL,
            SMART_SEARCH_LIMITS,
        )
        assert pattern == "hello world"

    @staticmethod
    def test_build_searcher_regex(sample_repo: Path) -> None:
        """Test building searcher for regex mode."""
        _searcher, pattern = build_candidate_searcher(
            sample_repo,
            "build.*graph",
            QueryMode.REGEX,
            SMART_SEARCH_LIMITS,
        )
        assert pattern == "build.*graph"


def test_search_rust_front_door_uses_rust_semantic_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rust searches should report non-unavailable semantic when adapter resolves payload."""
    (tmp_path / "lib.rs").write_text("pub fn compile_target() -> i32 { 1 }\n", encoding="utf-8")
    clear_caches()

    monkeypatch.setattr(
        "tools.cq.search.pipeline.search_semantic.enrich_with_language_semantics",
        lambda *_args, **_kwargs: LanguageSemanticEnrichmentOutcome(
            payload={
                "call_graph": {
                    "incoming_total": 1,
                    "outgoing_total": 0,
                    "incoming_callers": [{"name": "caller", "uri": "file:///x.rs"}],
                    "outgoing_callees": [],
                },
                "symbol_grounding": {"references": [{"uri": "file:///x.rs"}]},
            }
        ),
    )

    result = smart_search(tmp_path, "compile_target", lang_scope="rust")
    insight = cast("dict[str, object]", result.summary.get("front_door_insight", {}))
    degradation = cast("dict[str, object]", insight.get("degradation", {}))
    assert degradation.get("semantic") in {"ok", "partial"}
    rust_semantic = result.summary.get("rust_semantic_telemetry")
    assert isinstance(rust_semantic, SemanticTelemetryV1)
    assert rust_semantic.attempted >= 1


def test_search_python_capability_probe_unavailable_is_non_fatal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Python search should not fail solely due capability probe instability."""
    (tmp_path / "service.py").write_text(
        "def build_graph() -> int:\n    return 1\n",
        encoding="utf-8",
    )
    clear_caches()

    def capability_stub(*_args: object, **_kwargs: object) -> LanguageSemanticEnrichmentOutcome:
        return LanguageSemanticEnrichmentOutcome(
            payload={
                "coverage": {"status": "applied", "reason": None},
                "call_graph": {
                    "incoming_total": 0,
                    "outgoing_total": 0,
                    "incoming_callers": [],
                    "outgoing_callees": [],
                },
            }
        )

    monkeypatch.setattr(
        "tools.cq.search.pipeline.python_semantic.enrich_with_language_semantics",
        capability_stub,
    )
    monkeypatch.setattr(
        "tools.cq.search.pipeline.search_semantic.enrich_with_language_semantics",
        capability_stub,
    )

    result = smart_search(tmp_path, "build_graph", lang_scope="python")
    insight = cast("dict[str, object]", result.summary.get("front_door_insight", {}))
    degradation = cast("dict[str, object]", insight.get("degradation", {}))
    assert degradation.get("semantic") in {"ok", "partial"}
    python_semantic = result.summary.get("python_semantic_telemetry")
    assert isinstance(python_semantic, SemanticTelemetryV1)
    assert python_semantic.attempted >= 1
    assert python_semantic.applied >= 1


def test_search_python_timeout_reason_not_collapsed_to_session_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout outcomes should preserve request_timeout reason in diagnostics."""
    (tmp_path / "service.py").write_text(
        "def build_graph() -> int:\n    return 1\n",
        encoding="utf-8",
    )
    clear_caches()

    def timeout_stub(*_args: object, **_kwargs: object) -> LanguageSemanticEnrichmentOutcome:
        return LanguageSemanticEnrichmentOutcome(
            payload=None,
            timed_out=True,
            failure_reason="request_timeout",
        )

    monkeypatch.setattr(
        "tools.cq.search.pipeline.python_semantic.enrich_with_language_semantics",
        timeout_stub,
    )
    monkeypatch.setattr(
        "tools.cq.search.pipeline.search_semantic.enrich_with_language_semantics",
        timeout_stub,
    )

    result = smart_search(tmp_path, "build_graph", lang_scope="python")
    diagnostics = cast("list[object]", result.summary.get("python_semantic_diagnostics", []))
    assert diagnostics
    reasons = {
        str(cast("dict[str, object]", row).get("reason"))
        for row in diagnostics
        if isinstance(row, dict)
    }
    assert "request_timeout" in reasons
