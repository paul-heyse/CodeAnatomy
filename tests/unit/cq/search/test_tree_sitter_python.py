"""Tests for tree-sitter Python enrichment query packs."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter_python import (
    clear_tree_sitter_python_cache,
    enrich_python_context_by_byte_range,
    is_tree_sitter_python_available,
    lint_python_query_packs,
)


@pytest.fixture(autouse=True)
def _clear_tree_cache() -> None:
    clear_tree_sitter_python_cache()


@pytest.mark.skipif(
    not is_tree_sitter_python_available(),
    reason="tree-sitter-python is not available in this environment",
)
def test_lint_python_query_packs_has_no_errors() -> None:
    errors = lint_python_query_packs()
    assert errors == []


@pytest.mark.skipif(
    not is_tree_sitter_python_available(),
    reason="tree-sitter-python is not available in this environment",
)
def test_tree_sitter_resolution_fields_for_import_alias_span() -> None:
    source = (
        "import pkg.mod as alias\n"
        "\n"
        "def run() -> None:\n"
        "    value = alias.execute()\n"
        "    return None\n"
    )
    source_bytes = source.encode("utf-8")
    byte_start = source_bytes.index(b"alias")
    byte_end = byte_start + len(b"alias")
    payload = enrich_python_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key="sample.py",
    )
    assert payload is not None
    assert payload.get("enrichment_status") in {"applied", "degraded", "skipped"}
    chain = payload.get("import_alias_chain")
    assert isinstance(chain, list)
    assert chain
    assert any(isinstance(item, dict) and item.get("module") == "pkg.mod" for item in chain)


@pytest.mark.skipif(
    not is_tree_sitter_python_available(),
    reason="tree-sitter-python is not available in this environment",
)
def test_tree_sitter_resolution_fields_for_call_span() -> None:
    source = (
        "import pkg.mod as alias\n"
        "\n"
        "def run() -> None:\n"
        "    value = alias.execute()\n"
        "    return value\n"
    )
    source_bytes = source.encode("utf-8")
    byte_start = source_bytes.rindex(b"alias")
    byte_end = byte_start + len(b"alias")
    payload = enrich_python_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key="sample.py",
    )
    assert payload is not None
    assert payload.get("call_target") == "alias.execute"
    qualified = payload.get("qualified_name_candidates")
    assert isinstance(qualified, list)
    assert qualified
    binding = payload.get("binding_candidates")
    assert isinstance(binding, list)
    assert binding
