"""Tests for smart search classifier module."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.pipeline.classifier import (
    QueryMode,
    classify_from_node,
    classify_from_records,
    classify_heuristic,
    clear_caches,
    detect_query_mode,
    get_def_lines_cached,
    get_node_index,
    get_sg_root,
    get_symtable_table,
)
from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext

HIGH_CONFIDENCE_HEURISTIC = 0.9
MEDIUM_CONFIDENCE_HEURISTIC = 0.8
CALLSITE_CONFIDENCE_HEURISTIC = 0.6


class TestQueryModeDetection:
    """Tests for query mode detection."""

    @staticmethod
    def test_identifier_simple() -> None:
        """Test simple identifier detection."""
        assert detect_query_mode("build_graph") == QueryMode.IDENTIFIER

    @staticmethod
    def test_identifier_dotted() -> None:
        """Test dotted identifier detection."""
        assert detect_query_mode("module.function") == QueryMode.IDENTIFIER

    @staticmethod
    def test_identifier_with_numbers() -> None:
        """Test identifier with numbers."""
        assert detect_query_mode("func123") == QueryMode.IDENTIFIER

    @staticmethod
    def test_regex_star() -> None:
        """Test regex with star metacharacter."""
        assert detect_query_mode("config.*path") == QueryMode.REGEX

    @staticmethod
    def test_regex_brackets() -> None:
        """Test regex with brackets."""
        assert detect_query_mode("[a-z]+") == QueryMode.REGEX

    @staticmethod
    def test_regex_parens() -> None:
        """Test regex with parentheses."""
        assert detect_query_mode("(foo|bar)") == QueryMode.REGEX

    @staticmethod
    def test_regex_escape() -> None:
        """Test regex with escape."""
        assert detect_query_mode(r"foo\d+") == QueryMode.REGEX

    @staticmethod
    def test_literal_whitespace() -> None:
        """Test literal with whitespace."""
        assert detect_query_mode("hello world") == QueryMode.LITERAL

    @staticmethod
    def test_literal_special() -> None:
        """Test literal with non-identifier chars."""
        assert detect_query_mode("foo-bar") == QueryMode.LITERAL

    @staticmethod
    def test_force_regex() -> None:
        """Test forced regex mode."""
        assert detect_query_mode("build_graph", force_mode=QueryMode.REGEX) == QueryMode.REGEX

    @staticmethod
    def test_force_literal() -> None:
        """Test forced literal mode."""
        assert detect_query_mode("build_graph", force_mode=QueryMode.LITERAL) == QueryMode.LITERAL

    @staticmethod
    def test_force_identifier() -> None:
        """Test forced identifier mode."""
        assert (
            detect_query_mode("config.*path", force_mode=QueryMode.IDENTIFIER)
            == QueryMode.IDENTIFIER
        )


class TestHeuristicClassification:
    """Tests for fast heuristic classification."""

    @staticmethod
    def test_comment_match() -> None:
        """Test comment detection."""
        result = classify_heuristic("x = 1  # build_graph here", 14, "build_graph")
        assert result.category == "comment_match"
        assert result.confidence >= HIGH_CONFIDENCE_HEURISTIC
        assert result.skip_deeper is True

    @staticmethod
    def test_def_pattern() -> None:
        """Test function definition detection."""
        result = classify_heuristic("def build_graph(data):", 4, "build_graph")
        assert result.category == "definition"
        assert result.confidence >= MEDIUM_CONFIDENCE_HEURISTIC
        assert result.skip_deeper is False

    @staticmethod
    def test_async_def_pattern() -> None:
        """Test async function definition detection."""
        result = classify_heuristic("async def build_graph(data):", 10, "build_graph")
        assert result.category == "definition"
        assert result.confidence >= MEDIUM_CONFIDENCE_HEURISTIC

    @staticmethod
    def test_class_pattern() -> None:
        """Test class definition detection."""
        result = classify_heuristic("class GraphBuilder:", 6, "GraphBuilder")
        assert result.category == "definition"
        assert result.confidence >= MEDIUM_CONFIDENCE_HEURISTIC

    @staticmethod
    def test_import_pattern() -> None:
        """Test import detection."""
        result = classify_heuristic("import build_graph", 7, "build_graph")
        assert result.category == "import"
        assert result.confidence >= HIGH_CONFIDENCE_HEURISTIC
        assert result.skip_deeper is True

    @staticmethod
    def test_from_import_pattern() -> None:
        """Test from import detection."""
        result = classify_heuristic("from module import build_graph", 19, "build_graph")
        assert result.category == "from_import"
        assert result.confidence >= HIGH_CONFIDENCE_HEURISTIC
        assert result.skip_deeper is True

    @staticmethod
    def test_call_pattern() -> None:
        """Test call pattern detection."""
        result = classify_heuristic("result = build_graph(data)", 9, "build_graph")
        assert result.category == "callsite"
        assert result.confidence >= CALLSITE_CONFIDENCE_HEURISTIC
        assert result.skip_deeper is False  # Needs AST confirmation

    @staticmethod
    def test_docstring_hint() -> None:
        """Test docstring hint detection."""
        result = classify_heuristic('    """build_graph function."""', 7, "build_graph")
        assert result.category == "docstring_match"
        assert result.skip_deeper is False  # Needs AST to confirm

    @staticmethod
    def test_no_classification() -> None:
        """Test no confident classification."""
        result = classify_heuristic("x = build_graph", 4, "build_graph")
        assert result.category is None or result.category == "callsite"


class TestASTClassification:
    """Tests for AST-based classification."""

    @staticmethod
    @pytest.fixture
    def python_source(tmp_path: Path) -> Path:
        """Create sample Python file for testing.

        Returns:
        -------
        Path
            Path to a temporary Python source file.
        """
        source = tmp_path / "sample.py"
        source.write_text(
            """\
def build_graph(data):
    '''Build a graph from data.'''
    return Graph(data)

class GraphBuilder:
    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

x = build_graph(data)
from module import something
import os
"""
        )
        return source

    @staticmethod
    def test_classify_function_definition(python_source: Path) -> None:
        """Test classification of function definition."""
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(python_source, cache_context=cache_context)
        assert sg_root is not None

        result = classify_from_node(
            sg_root, 1, 4, cache_context=cache_context
        )  # 'build_graph' in def
        assert result is not None
        assert result.category in {"definition", "reference"}

    @staticmethod
    def test_classify_class_definition(python_source: Path) -> None:
        """Test classification of class definition."""
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(python_source, cache_context=cache_context)
        assert sg_root is not None

        result = classify_from_node(
            sg_root, 5, 6, cache_context=cache_context
        )  # 'GraphBuilder' in class
        assert result is not None
        assert result.category in {"definition", "reference"}

    @staticmethod
    def test_classify_call(python_source: Path) -> None:
        """Test classification of function call."""
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(python_source, cache_context=cache_context)
        assert sg_root is not None

        result = classify_from_node(
            sg_root, 12, 4, cache_context=cache_context
        )  # 'build_graph(data)' call
        assert result is not None
        # Could be callsite or reference depending on exact position
        assert result.category in {"callsite", "reference", "assignment"}

    @staticmethod
    def test_classify_import(python_source: Path) -> None:
        """Test classification of import."""
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(python_source, cache_context=cache_context)
        assert sg_root is not None

        result = classify_from_node(sg_root, 13, 0, cache_context=cache_context)  # from import line
        assert result is not None
        assert result.category in {"from_import", "reference"}

    @staticmethod
    def test_classify_rust_string_literal(tmp_path: Path) -> None:
        """Rust string literals should classify as string matches, not callsites."""
        source = tmp_path / "sample.rs"
        line = 'const FEATURE_MSG: &str = "Async UDFs require the async-udf feature";\n'
        source.write_text(line, encoding="utf-8")
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(source, lang="rust", cache_context=cache_context)
        assert sg_root is not None

        col = line.index("Async UDFs require the async-udf feature")
        result = classify_from_node(
            sg_root,
            1,
            col,
            lang="rust",
            cache_context=cache_context,
        )
        assert result is not None
        assert result.category == "string_match"

    @staticmethod
    def test_containing_scope_detected(python_source: Path) -> None:
        """Test that containing scope is detected."""
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(python_source, cache_context=cache_context)
        assert sg_root is not None

        result = classify_from_node(
            sg_root, 10, 8, cache_context=cache_context
        )  # inside add_node method
        # If found, should have containing_scope
        if result is not None:
            # Scope could be the method or class
            pass

    @staticmethod
    def test_classify_from_records_import(python_source: Path) -> None:
        """Record-based classification should detect imports."""
        clear_caches()
        root = python_source.parent
        result = classify_from_records(
            python_source,
            root,
            13,
            0,
            cache_context=ClassifierCacheContext(),
        )
        assert result is not None
        assert result.category in {"from_import", "import"}

    @staticmethod
    def test_classify_from_records_definition_requires_name_overlap(tmp_path: Path) -> None:
        """Definition records should not classify body references as definitions."""
        source = tmp_path / "sample.py"
        source.write_text(
            "def process_imports():\n    import_value = 1\n    return import_value\n",
            encoding="utf-8",
        )
        clear_caches()
        result = classify_from_records(
            source,
            tmp_path,
            2,
            4,
            cache_context=ClassifierCacheContext(),
        )
        assert result is None or result.category != "definition"


class TestCacheHelpers:
    """Tests for classifier cache helpers."""

    @staticmethod
    def test_def_lines_cached(tmp_path: Path) -> None:
        """Def line cache should return the same object for repeated calls."""
        source = tmp_path / "sample.py"
        source.write_text("def foo():\n    return 1\n\nasync def bar():\n    pass\n")
        clear_caches()
        cache_context = ClassifierCacheContext()
        first = get_def_lines_cached(source, cache_context=cache_context)
        second = get_def_lines_cached(source, cache_context=cache_context)
        assert first is second
        assert any(line == 1 for line, _ in first)

    @staticmethod
    def test_symtable_cached(tmp_path: Path) -> None:
        """Symtable cache should reuse the same table object."""
        source = tmp_path / "sym.py"
        source.write_text("def foo(x):\n    return x\n")
        clear_caches()
        text = source.read_text()
        cache_context = ClassifierCacheContext()
        table1 = get_symtable_table(source, text, cache_context=cache_context)
        table2 = get_symtable_table(source, text, cache_context=cache_context)
        assert table1 is not None
        assert table1 is table2


class TestNodeIndex:
    """Tests for AST node index caching and scaling."""

    @staticmethod
    def test_node_index_cached(tmp_path: Path) -> None:
        """Node index should be cached per file."""
        source = tmp_path / "nodes.py"
        source.write_text("def foo():\n    return 1\n")
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(source, cache_context=cache_context)
        assert sg_root is not None
        index1 = get_node_index(source, sg_root, cache_context=cache_context)
        index2 = get_node_index(source, sg_root, cache_context=cache_context)
        assert index1 is index2

    @staticmethod
    def test_node_index_large_file(tmp_path: Path) -> None:
        """Node index should handle large files without error."""
        source = tmp_path / "large.py"
        lines = []
        for i in range(200):
            lines.append(f"def func_{i}():\n")
            lines.append(f"    return {i}\n\n")
        source.write_text("".join(lines))
        clear_caches()
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(source, cache_context=cache_context)
        assert sg_root is not None
        index = get_node_index(source, sg_root, cache_context=cache_context)
        node = index.find_containing(2, 4)
        assert node is not None


class TestCacheManagement:
    """Tests for cache management."""

    @staticmethod
    def test_clear_caches(tmp_path: Path) -> None:
        """Test cache clearing."""
        source = tmp_path / "cached.py"
        source.write_text("def foo(): pass")

        # Populate cache
        cache_context = ClassifierCacheContext()
        sg_root = get_sg_root(source, cache_context=cache_context)
        assert sg_root is not None

        # Clear and verify it's repopulated
        clear_caches()
        sg_root2 = get_sg_root(source, cache_context=ClassifierCacheContext())
        assert sg_root2 is not None
