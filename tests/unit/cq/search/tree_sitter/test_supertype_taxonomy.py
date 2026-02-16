"""Tests for runtime supertype taxonomy extraction."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tools.cq.search.tree_sitter.core import language_registry as registry
from tools.cq.search.tree_sitter.schema.node_schema import (
    SupertypeIndexV1,
    build_supertype_index,
)

DECLARATION_SUPERTYPE_ID = 10


def test_build_supertype_index_from_runtime_language() -> None:
    """Construct supertype index from runtime supertypes and subtype relations."""
    def _subtypes(supertype_id: int) -> tuple[int, ...]:
        return (11, 12) if supertype_id == DECLARATION_SUPERTYPE_ID else ()

    def _node_kind(kind_id: int) -> str | None:
        return {
            10: "declaration",
            11: "function_definition",
            12: "class_definition",
        }.get(kind_id)

    language = SimpleNamespace(
        supertypes=(10,),
        subtypes=_subtypes,
        node_kind_for_id=_node_kind,
    )
    rows = build_supertype_index(language)
    assert rows == (
        SupertypeIndexV1(
            supertype_id=10,
            supertype="declaration",
            subtype_ids=(11, 12),
            subtypes=("function_definition", "class_definition"),
        ),
    )


def test_load_language_registry_prefers_schema_supertype_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test load language registry prefers schema supertype index."""
    registry.load_language_registry.cache_clear()
    monkeypatch.setattr(
        registry,
        "load_grammar_schema",
        lambda _language: SimpleNamespace(
            node_types=(),
            supertype_index=(
                SimpleNamespace(supertype="declaration"),
                SimpleNamespace(supertype="expression"),
            ),
        ),
    )
    monkeypatch.setattr(
        registry,
        "load_tree_sitter_language",
        lambda _language: SimpleNamespace(
            name="python",
            semantic_version=(0, 25, 0),
            abi_version=15,
        ),
    )
    row = registry.load_language_registry("python")
    assert row is not None
    assert row.supertypes == ("declaration", "expression")
    registry.load_language_registry.cache_clear()
