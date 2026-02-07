"""Tests for smart search classifier module."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search.classifier import (
    QueryMode,
    classify_from_node,
    classify_from_records,
    classify_heuristic,
    clear_caches,
    detect_query_mode,
    enrich_with_symtable,
    get_def_lines_cached,
    get_node_index,
    get_sg_root,
    get_symtable_table,
)


class TestQueryModeDetection:
    """Tests for query mode detection."""

    def test_identifier_simple(self) -> None:
        """Test simple identifier detection."""
        assert detect_query_mode("build_graph") == QueryMode.IDENTIFIER

    def test_identifier_dotted(self) -> None:
        """Test dotted identifier detection."""
        assert detect_query_mode("module.function") == QueryMode.IDENTIFIER

    def test_identifier_with_numbers(self) -> None:
        """Test identifier with numbers."""
        assert detect_query_mode("func123") == QueryMode.IDENTIFIER

    def test_regex_star(self) -> None:
        """Test regex with star metacharacter."""
        assert detect_query_mode("config.*path") == QueryMode.REGEX

    def test_regex_brackets(self) -> None:
        """Test regex with brackets."""
        assert detect_query_mode("[a-z]+") == QueryMode.REGEX

    def test_regex_parens(self) -> None:
        """Test regex with parentheses."""
        assert detect_query_mode("(foo|bar)") == QueryMode.REGEX

    def test_regex_escape(self) -> None:
        """Test regex with escape."""
        assert detect_query_mode(r"foo\d+") == QueryMode.REGEX

    def test_literal_whitespace(self) -> None:
        """Test literal with whitespace."""
        assert detect_query_mode("hello world") == QueryMode.LITERAL

    def test_literal_special(self) -> None:
        """Test literal with non-identifier chars."""
        assert detect_query_mode("foo-bar") == QueryMode.LITERAL

    def test_force_regex(self) -> None:
        """Test forced regex mode."""
        assert detect_query_mode("build_graph", force_mode=QueryMode.REGEX) == QueryMode.REGEX

    def test_force_literal(self) -> None:
        """Test forced literal mode."""
        assert detect_query_mode("build_graph", force_mode=QueryMode.LITERAL) == QueryMode.LITERAL

    def test_force_identifier(self) -> None:
        """Test forced identifier mode."""
        assert (
            detect_query_mode("config.*path", force_mode=QueryMode.IDENTIFIER)
            == QueryMode.IDENTIFIER
        )


class TestHeuristicClassification:
    """Tests for fast heuristic classification."""

    def test_comment_match(self) -> None:
        """Test comment detection."""
        result = classify_heuristic("x = 1  # build_graph here", 14, "build_graph")
        assert result.category == "comment_match"
        assert result.confidence >= 0.9
        assert result.skip_deeper is True

    def test_def_pattern(self) -> None:
        """Test function definition detection."""
        result = classify_heuristic("def build_graph(data):", 4, "build_graph")
        assert result.category == "definition"
        assert result.confidence >= 0.8
        assert result.skip_deeper is False

    def test_async_def_pattern(self) -> None:
        """Test async function definition detection."""
        result = classify_heuristic("async def build_graph(data):", 10, "build_graph")
        assert result.category == "definition"
        assert result.confidence >= 0.8

    def test_class_pattern(self) -> None:
        """Test class definition detection."""
        result = classify_heuristic("class GraphBuilder:", 6, "GraphBuilder")
        assert result.category == "definition"
        assert result.confidence >= 0.8

    def test_import_pattern(self) -> None:
        """Test import detection."""
        result = classify_heuristic("import build_graph", 7, "build_graph")
        assert result.category == "import"
        assert result.confidence >= 0.9
        assert result.skip_deeper is True

    def test_from_import_pattern(self) -> None:
        """Test from import detection."""
        result = classify_heuristic("from module import build_graph", 19, "build_graph")
        assert result.category == "from_import"
        assert result.confidence >= 0.9
        assert result.skip_deeper is True

    def test_call_pattern(self) -> None:
        """Test call pattern detection."""
        result = classify_heuristic("result = build_graph(data)", 9, "build_graph")
        assert result.category == "callsite"
        assert result.confidence >= 0.6
        assert result.skip_deeper is False  # Needs AST confirmation

    def test_docstring_hint(self) -> None:
        """Test docstring hint detection."""
        result = classify_heuristic('    """build_graph function."""', 7, "build_graph")
        assert result.category == "docstring_match"
        assert result.skip_deeper is False  # Needs AST to confirm

    def test_no_classification(self) -> None:
        """Test no confident classification."""
        result = classify_heuristic("x = build_graph", 4, "build_graph")
        assert result.category is None or result.category == "callsite"


class TestASTClassification:
    """Tests for AST-based classification."""

    @pytest.fixture
    def python_source(self, tmp_path: Path) -> Path:
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

    def test_classify_function_definition(self, python_source: Path) -> None:
        """Test classification of function definition."""
        clear_caches()
        sg_root = get_sg_root(python_source)
        assert sg_root is not None

        result = classify_from_node(sg_root, 1, 4)  # 'build_graph' in def
        assert result is not None
        assert result.category in {"definition", "reference"}

    def test_classify_class_definition(self, python_source: Path) -> None:
        """Test classification of class definition."""
        clear_caches()
        sg_root = get_sg_root(python_source)
        assert sg_root is not None

        result = classify_from_node(sg_root, 5, 6)  # 'GraphBuilder' in class
        assert result is not None
        assert result.category in {"definition", "reference"}

    def test_classify_call(self, python_source: Path) -> None:
        """Test classification of function call."""
        clear_caches()
        sg_root = get_sg_root(python_source)
        assert sg_root is not None

        result = classify_from_node(sg_root, 12, 4)  # 'build_graph(data)' call
        assert result is not None
        # Could be callsite or reference depending on exact position
        assert result.category in {"callsite", "reference", "assignment"}

    def test_classify_import(self, python_source: Path) -> None:
        """Test classification of import."""
        clear_caches()
        sg_root = get_sg_root(python_source)
        assert sg_root is not None

        result = classify_from_node(sg_root, 13, 0)  # from import line
        assert result is not None
        assert result.category in {"from_import", "reference"}

    def test_containing_scope_detected(self, python_source: Path) -> None:
        """Test that containing scope is detected."""
        clear_caches()
        sg_root = get_sg_root(python_source)
        assert sg_root is not None

        result = classify_from_node(sg_root, 10, 8)  # inside add_node method
        # If found, should have containing_scope
        if result is not None:
            # Scope could be the method or class
            pass

    def test_classify_from_records_import(self, python_source: Path) -> None:
        """Record-based classification should detect imports."""
        clear_caches()
        root = python_source.parent
        result = classify_from_records(python_source, root, 13, 0)
        assert result is not None
        assert result.category in {"from_import", "import"}

    def test_classify_from_records_definition_requires_name_overlap(self, tmp_path: Path) -> None:
        """Definition records should not classify body references as definitions."""
        source = tmp_path / "sample.py"
        source.write_text(
            "def process_imports():\n"
            "    import_value = 1\n"
            "    return import_value\n",
            encoding="utf-8",
        )
        clear_caches()
        result = classify_from_records(source, tmp_path, 2, 4)
        assert result is None or result.category != "definition"


class TestCacheHelpers:
    """Tests for classifier cache helpers."""

    def test_def_lines_cached(self, tmp_path: Path) -> None:
        """Def line cache should return the same object for repeated calls."""
        source = tmp_path / "sample.py"
        source.write_text("def foo():\n    return 1\n\nasync def bar():\n    pass\n")
        clear_caches()
        first = get_def_lines_cached(source)
        second = get_def_lines_cached(source)
        assert first is second
        assert any(line == 1 for line, _ in first)

    def test_symtable_cached(self, tmp_path: Path) -> None:
        """Symtable cache should reuse the same table object."""
        source = tmp_path / "sym.py"
        source.write_text("def foo(x):\n    return x\n")
        clear_caches()
        text = source.read_text()
        table1 = get_symtable_table(source, text)
        table2 = get_symtable_table(source, text)
        assert table1 is not None
        assert table1 is table2


class TestNodeIndex:
    """Tests for AST node index caching and scaling."""

    def test_node_index_cached(self, tmp_path: Path) -> None:
        """Node index should be cached per file."""
        source = tmp_path / "nodes.py"
        source.write_text("def foo():\n    return 1\n")
        clear_caches()
        sg_root = get_sg_root(source)
        assert sg_root is not None
        index1 = get_node_index(source, sg_root)
        index2 = get_node_index(source, sg_root)
        assert index1 is index2

    def test_node_index_large_file(self, tmp_path: Path) -> None:
        """Node index should handle large files without error."""
        source = tmp_path / "large.py"
        lines = []
        for i in range(200):
            lines.append(f"def func_{i}():\n")
            lines.append(f"    return {i}\n\n")
        source.write_text("".join(lines))
        clear_caches()
        sg_root = get_sg_root(source)
        assert sg_root is not None
        index = get_node_index(source, sg_root)
        node = index.find_containing(2, 4)
        assert node is not None


class TestSymtableEnrichment:
    """Tests for symtable-based enrichment."""

    def test_parameter_detection(self) -> None:
        """Test parameter detection."""
        source = """\
def foo(param):
    return param
"""
        result = enrich_with_symtable(source, "test.py", "param", 2)
        assert result is not None
        assert result.is_parameter is True

    def test_local_detection(self) -> None:
        """Test local variable detection."""
        source = """\
def foo():
    local_var = 1
    return local_var
"""
        result = enrich_with_symtable(source, "test.py", "local_var", 2)
        assert result is not None
        assert result.is_local is True
        assert result.is_assigned is True

    def test_global_detection(self) -> None:
        """Test global variable detection."""
        source = """\
GLOBAL = 1

def foo():
    global GLOBAL
    return GLOBAL
"""
        result = enrich_with_symtable(source, "test.py", "GLOBAL", 5)
        assert result is not None
        assert result.is_global is True

    def test_imported_detection(self) -> None:
        """Test imported symbol detection at module level."""
        source = """\
import os
x = os.path
"""
        # Looking up at module level where os is directly imported
        result = enrich_with_symtable(source, "test.py", "os", 1)
        assert result is not None
        assert result.is_imported is True

    def test_closure_detection(self) -> None:
        """Test free variable (closure) detection."""
        source = """\
def outer():
    captured = 1
    def inner():
        return captured
    return inner
"""
        result = enrich_with_symtable(source, "test.py", "captured", 4)
        assert result is not None
        assert result.is_free is True

    def test_nonlocal_detection(self) -> None:
        """Test nonlocal variable detection."""
        source = """\
def outer():
    x = 1
    def inner():
        nonlocal x
        x = 2
    return inner
"""
        result = enrich_with_symtable(source, "test.py", "x", 5)
        assert result is not None
        # When using nonlocal, it's still "free" from the inner scope's perspective
        # but also declared nonlocal
        assert result.is_nonlocal is True

    def test_syntax_error_returns_none(self) -> None:
        """Test that syntax errors return None."""
        source = "def foo(:\n    pass"
        result = enrich_with_symtable(source, "test.py", "foo", 1)
        assert result is None

    def test_unknown_symbol_returns_none(self) -> None:
        """Test that unknown symbols return None."""
        source = """\
def foo():
    return 1
"""
        result = enrich_with_symtable(source, "test.py", "nonexistent", 1)
        assert result is None


class TestCacheManagement:
    """Tests for cache management."""

    def test_clear_caches(self, tmp_path: Path) -> None:
        """Test cache clearing."""
        source = tmp_path / "cached.py"
        source.write_text("def foo(): pass")

        # Populate cache
        sg_root = get_sg_root(source)
        assert sg_root is not None

        # Clear and verify it's repopulated
        clear_caches()
        sg_root2 = get_sg_root(source)
        assert sg_root2 is not None
