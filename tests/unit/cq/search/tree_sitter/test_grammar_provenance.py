"""Tests for grammar provenance stamping across tree-sitter contracts."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from tools.cq.search.tree_sitter.core import language_registry as registry
from tools.cq.search.tree_sitter.query.planner import build_pack_plan
from tools.cq.search.tree_sitter.schema import node_schema

GRAMMAR_ABI_VERSION = 15


def test_load_grammar_schema_runtime_stamps_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load grammar schema runtime stamps provenance."""
    rows = (node_schema.GrammarNodeTypeV1(type="identifier", named=True),)
    monkeypatch.setattr(node_schema, "_load_runtime_node_types", lambda _language: rows)
    monkeypatch.setattr(
        node_schema,
        "_runtime_language",
        lambda _language: SimpleNamespace(
            name="python",
            semantic_version=(0, 25, 0),
            abi_version=15,
        ),
    )
    schema = node_schema.load_grammar_schema("python")
    assert schema is not None
    assert schema.grammar_name == "python"
    assert schema.semantic_version == (0, 25, 0)
    assert schema.abi_version == GRAMMAR_ABI_VERSION


def test_load_language_registry_stamps_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load language registry stamps provenance."""
    registry.load_language_registry.cache_clear()
    monkeypatch.setattr(
        registry,
        "load_grammar_schema",
        lambda _language: node_schema.GrammarSchemaV1(
            language="python",
            node_types=(node_schema.GrammarNodeTypeV1(type="identifier", named=True),),
        ),
    )
    monkeypatch.setattr(
        registry,
        "load_tree_sitter_language",
        lambda _language: SimpleNamespace(
            name="python",
            semantic_version=(0, 25, 0),
            abi_version=15,
            supertypes=(),
            node_kind_for_id=lambda _idx: None,
        ),
    )
    row = registry.load_language_registry("python")
    assert row is not None
    assert row.grammar_name == "python"
    assert row.semantic_version == (0, 25, 0)
    assert row.abi_version == GRAMMAR_ABI_VERSION
    registry.load_language_registry.cache_clear()


def test_build_pack_plan_stamps_query_language_provenance() -> None:
    """Test build pack plan stamps query language provenance."""
    query_language = SimpleNamespace(
        name="python",
        semantic_version=(0, 25, 0),
        abi_version=15,
    )
    query = SimpleNamespace(
        pattern_count=0,
        capture_count=0,
        language=query_language,
    )
    plan = build_pack_plan(
        pack_name="unit.scm",
        query=cast("Any", query),
        query_text="(identifier) @id",
    )
    assert plan.grammar_name == "python"
    assert plan.semantic_version == (0, 25, 0)
    assert plan.abi_version == GRAMMAR_ABI_VERSION
