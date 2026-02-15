"""Tests for shared tree-sitter query compiler."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.query.compiler import compile_query

pytestmark = pytest.mark.skipif(
    not hasattr(pytest, "tree_sitter_available") or not pytest.tree_sitter_available,
    reason="tree-sitter not available",
)


def test_compile_query_python() -> None:
    """Test compiling a simple Python query."""
    source = "(function_definition) @fn"
    query = compile_query(
        language="python",
        pack_name="test.scm",
        source=source,
        request_surface="artifact",
        validate_rules=False,
    )
    assert query is not None
    assert hasattr(query, "pattern_count")


def test_compile_query_rust() -> None:
    """Test compiling a simple Rust query."""
    source = "(function_item) @fn"
    query = compile_query(
        language="rust",
        pack_name="test.scm",
        source=source,
        request_surface="artifact",
        validate_rules=False,
    )
    assert query is not None
    assert hasattr(query, "pattern_count")


def test_compile_query_specialization() -> None:
    """Test query specialization with different surfaces."""
    source = """
    (function_definition
      name: (identifier) @name
      (#set! cq.surface "artifact_only")
    )
    """
    artifact_query = compile_query(
        language="python",
        pack_name="test.scm",
        source=source,
        request_surface="artifact",
        validate_rules=False,
    )
    terminal_query = compile_query(
        language="python",
        pack_name="test.scm",
        source=source,
        request_surface="terminal",
        validate_rules=False,
    )
    assert artifact_query is not None
    assert terminal_query is not None


def test_compile_query_validation_rooted() -> None:
    """Test rooted validation enforcement."""
    non_rooted_source = "(_ (identifier) @id)"
    with pytest.raises(ValueError, match="not rooted"):
        compile_query(
            language="python",
            pack_name="test.scm",
            source=non_rooted_source,
            request_surface="artifact",
            validate_rules=True,
        )


def test_compile_query_caching() -> None:
    """Test that compiled queries are cached."""
    source = "(function_definition) @fn"
    query1 = compile_query(
        language="python",
        pack_name="test.scm",
        source=source,
        request_surface="artifact",
        validate_rules=False,
    )
    query2 = compile_query(
        language="python",
        pack_name="test.scm",
        source=source,
        request_surface="artifact",
        validate_rules=False,
    )
    assert query1 is query2
