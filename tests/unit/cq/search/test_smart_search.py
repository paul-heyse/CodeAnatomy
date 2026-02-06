"""Tests for smart search pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from tools.cq.core.locations import SourceSpan
from tools.cq.search.classifier import QueryMode, clear_caches
from tools.cq.search.smart_search import (
    SMART_SEARCH_LIMITS,
    EnrichedMatch,
    RawMatch,
    SearchStats,
    build_candidate_searcher,
    build_finding,
    build_followups,
    build_sections,
    build_summary,
    classify_match,
    compute_relevance_score,
    smart_search,
)
from tools.cq.search.tree_sitter_rust import is_tree_sitter_rust_available


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

    def test_raw_match_creation(self) -> None:
        """Test RawMatch creation."""
        raw = RawMatch(
            span=_span("src/module.py", 10, 9, 20),
            text="result = build_graph(data)",
            match_text="build_graph",
            match_start=9,
            match_end=20,
        )
        assert raw.file == "src/module.py"
        assert raw.line == 10
        assert raw.col == 9
        assert raw.match_text == "build_graph"

    def test_raw_match_no_context_fields(self) -> None:
        """RawMatch should not store line-context payloads."""
        raw = RawMatch(
            span=_span("src/module.py", 10, 9, 20),
            text="result = build_graph(data)",
            match_text="build_graph",
            match_start=9,
            match_end=20,
        )
        assert not hasattr(raw, "context_before")
        assert not hasattr(raw, "context_after")


class TestSearchStats:
    """Tests for SearchStats struct."""

    def test_search_stats_basic(self) -> None:
        """Test basic SearchStats."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=10,
            total_matches=50,
        )
        assert stats.scanned_files == 100
        assert stats.matched_files == 10
        assert stats.truncated is False
        assert stats.timed_out is False
        assert stats.max_files_hit is False
        assert stats.max_matches_hit is False

    def test_search_stats_truncated(self) -> None:
        """Test truncated SearchStats."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=10,
            total_matches=500,
            truncated=True,
            max_matches_hit=True,
        )
        assert stats.truncated is True
        assert stats.max_matches_hit is True


class TestEnrichedMatch:
    """Tests for EnrichedMatch struct."""

    def test_enriched_match_creation(self) -> None:
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
        assert match.confidence == 0.95
        assert match.evidence_kind == "resolved_ast"


class TestRelevanceScoring:
    """Tests for relevance scoring."""

    def test_definition_scores_highest(self) -> None:
        """Test that definitions score highest."""
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

    def test_src_files_score_higher_than_tests(self) -> None:
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

    def test_comment_match_scores_low(self) -> None:
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

    def test_classify_comment(self, sample_repo: Path) -> None:
        """Test classification of comment match."""
        clear_caches()
        raw = RawMatch(
            span=_span("src/module_a.py", 1, 2, 13),
            text="# build_graph is here",
            match_text="build_graph",
            match_start=2,
            match_end=13,
        )
        # Create file with comment
        (sample_repo / "src" / "module_a.py").write_text("# build_graph is here\n")
        enriched = classify_match(raw, sample_repo)
        assert enriched.category == "comment_match"
        assert enriched.evidence_kind == "heuristic"

    def test_classify_import(self, sample_repo: Path) -> None:
        """Test classification of import match."""
        clear_caches()
        raw = RawMatch(
            span=_span("src/module_a.py", 1, 7, 18),
            text="import build_graph",
            match_text="build_graph",
            match_start=7,
            match_end=18,
        )
        # Create file with import
        (sample_repo / "src" / "module_a.py").write_text("import build_graph\n")
        enriched = classify_match(raw, sample_repo)
        assert enriched.category == "import"


class TestBuildFinding:
    """Tests for Finding construction."""

    def test_build_finding_basic(self, sample_repo: Path) -> None:
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
        assert finding.anchor.line == 10

    def test_build_finding_with_scope(self, sample_repo: Path) -> None:
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

    def test_followups_for_identifier_with_defs(self) -> None:
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

    def test_followups_for_identifier_with_calls(self) -> None:
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

    def test_no_followups_for_regex(self) -> None:
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

    def test_summary_basic(self) -> None:
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
        summary = build_summary(
            "build_graph",
            QueryMode.IDENTIFIER,
            stats,
            matches,
            SMART_SEARCH_LIMITS,
        )
        assert summary["query"] == "build_graph"
        assert summary["mode"] == "identifier"
        assert summary["scanned_files"] == 100
        assert summary["scanned_files_is_estimate"] is True
        assert summary["matched_files"] == 10
        assert summary["returned_matches"] == 1

    def test_summary_with_truncation(self) -> None:
        """Test summary with truncation."""
        stats = SearchStats(
            scanned_files=100,
            matched_files=10,
            total_matches=500,
            truncated=True,
            max_matches_hit=True,
        )
        summary = build_summary(
            "build_graph",
            QueryMode.IDENTIFIER,
            stats,
            [],
            SMART_SEARCH_LIMITS,
        )
        assert summary["truncated"] is True
        assert summary["caps_hit"] == "max_total_matches"

    def test_summary_multilang_contract_keys(self) -> None:
        """Summary should expose canonical multilang contract keys."""
        stats = SearchStats(
            scanned_files=1,
            matched_files=1,
            total_matches=1,
        )
        summary = build_summary(
            "build_graph",
            QueryMode.IDENTIFIER,
            stats,
            [],
            SMART_SEARCH_LIMITS,
        )
        assert summary["lang_scope"] == "auto"
        assert summary["language_order"] == ["python", "rust"]
        assert isinstance(summary["languages"], dict)
        assert isinstance(summary["cross_language_diagnostics"], list)


class TestBuildSections:
    """Tests for section construction."""

    def test_sections_include_top_contexts(self, sample_repo: Path) -> None:
        """Test that Top Contexts section is included."""
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
        assert "Top Contexts" in titles

    def test_sections_include_definitions_for_identifier(self, sample_repo: Path) -> None:
        """Test that Definitions section is included for identifier mode."""
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
        assert "Definitions" in titles

    def test_non_code_matches_collapsed(self, sample_repo: Path) -> None:
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

    def test_sections_group_by_scope(self, sample_repo: Path) -> None:
        """Top contexts should group by containing scope within a file."""
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
        top_contexts = sections[0]
        messages = {f.message for f in top_contexts.findings}
        assert "helper (src/module.py)" in messages
        assert "worker (src/module.py)" in messages
        assert len(top_contexts.findings) == 2

    def test_sections_include_strings_when_enabled(self, sample_repo: Path) -> None:
        """Non-code matches should only appear in top contexts when include_strings is set."""
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
        assert sections[0].findings == []
        sections_with_strings = build_sections(
            matches,
            sample_repo,
            "build_graph",
            QueryMode.IDENTIFIER,
            include_strings=True,
        )
        assert len(sections_with_strings[0].findings) == 1


class TestSmartSearch:
    """Tests for the full smart search pipeline."""

    def test_smart_search_identifier(self, sample_repo: Path) -> None:
        """Test smart search with identifier mode."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        assert result.run.macro == "search"
        assert "query" in result.summary
        assert result.summary["mode"] == "identifier"
        # Should find matches
        assert len(result.evidence) > 0

    def test_smart_search_scanned_files_exact(self, sample_repo: Path) -> None:
        """Smart search should report exact scanned file counts when available."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        summary = cast("Mapping[str, object]", result.summary)
        assert summary["scanned_files_is_estimate"] is False
        scanned_files = cast("int", summary["scanned_files"])
        matched_files = cast("int", summary["matched_files"])
        assert scanned_files >= matched_files

    def test_smart_search_with_include_globs(self, sample_repo: Path) -> None:
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

    def test_smart_search_sections_present(self, sample_repo: Path) -> None:
        """Test that sections are present in result."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        assert len(result.sections) > 0
        # Top Contexts should always be first
        assert result.sections[0].title == "Top Contexts"

    def test_smart_search_key_findings(self, sample_repo: Path) -> None:
        """Test that key findings are populated."""
        clear_caches()
        result = smart_search(sample_repo, "build_graph")
        # Key findings should be populated from top contexts
        assert len(result.key_findings) > 0 or len(result.sections) == 0

    def test_evidence_cap(self, sample_repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Evidence should respect MAX_EVIDENCE cap."""
        import importlib

        smart_search_module = importlib.import_module("tools.cq.search.smart_search")

        clear_caches()
        monkeypatch.setattr(smart_search_module, "MAX_EVIDENCE", 1)
        result = smart_search_module.smart_search(sample_repo, "build_graph")
        assert len(result.evidence) <= 1

    def test_multilang_order_is_python_first(self, tmp_path: Path) -> None:
        """Merged evidence should prefer Python before Rust for tied relevance."""
        (tmp_path / "a.py").write_text("def build_graph():\n    return 1\n", encoding="utf-8")
        (tmp_path / "b.rs").write_text("fn build_graph() -> i32 { 1 }\n", encoding="utf-8")
        clear_caches()
        result = smart_search(tmp_path, "build_graph")
        summary = cast("Mapping[str, object]", result.summary)
        assert summary["language_order"] == ["python", "rust"]
        assert result.evidence
        assert result.evidence[0].details.get("language") == "python"

    def test_cross_language_warning_for_python_intent_rust_only(self, tmp_path: Path) -> None:
        """Python-oriented text with Rust-only matches should emit a warning."""
        (tmp_path / "only.rs").write_text("// decorator\nfn f() {}\n", encoding="utf-8")
        clear_caches()
        result = smart_search(tmp_path, "decorator")
        diagnostics = cast("list[object]", result.summary["cross_language_diagnostics"])
        assert diagnostics
        assert any(
            "Python-oriented query produced no Python matches" in str(item) for item in diagnostics
        )

    @pytest.mark.skipif(
        not is_tree_sitter_rust_available(),
        reason="tree-sitter-rust is not available in this environment",
    )
    def test_rust_tree_sitter_enrichment_attached(self, tmp_path: Path) -> None:
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
        assert "rust_tree_sitter" in detail_data

    def test_rust_tree_sitter_fail_open(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Tree-sitter enrichment failures must not break core search results."""
        (tmp_path / "lib.rs").write_text(
            'fn build_graph() {\n    println!("x");\n}\n',
            encoding="utf-8",
        )
        clear_caches()
        import importlib

        smart_search_module = importlib.import_module("tools.cq.search.smart_search")

        def _boom(*_args: object, **_kwargs: object) -> dict[str, object]:
            msg = "forced enrichment failure"
            raise RuntimeError(msg)

        monkeypatch.setattr(smart_search_module, "enrich_rust_context", _boom)
        result = smart_search_module.smart_search(tmp_path, "build_graph", lang_scope="rust")
        assert result.summary["query"] == "build_graph"
        assert result.evidence


class TestCandidateSearcher:
    """Tests for candidate searcher construction."""

    def test_build_searcher_identifier(self, sample_repo: Path) -> None:
        """Test building searcher for identifier mode."""
        _searcher, pattern = build_candidate_searcher(
            sample_repo,
            "build_graph",
            QueryMode.IDENTIFIER,
            SMART_SEARCH_LIMITS,
        )
        assert r"\b" in pattern  # Word boundary
        assert "build_graph" in pattern

    def test_build_searcher_literal(self, sample_repo: Path) -> None:
        """Test building searcher for literal mode."""
        _searcher, pattern = build_candidate_searcher(
            sample_repo,
            "hello world",
            QueryMode.LITERAL,
            SMART_SEARCH_LIMITS,
        )
        assert pattern == "hello world"

    def test_build_searcher_regex(self, sample_repo: Path) -> None:
        """Test building searcher for regex mode."""
        _searcher, pattern = build_candidate_searcher(
            sample_repo,
            "build.*graph",
            QueryMode.REGEX,
            SMART_SEARCH_LIMITS,
        )
        assert pattern == "build.*graph"
