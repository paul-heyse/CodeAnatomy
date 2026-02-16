"""Tests for query IR planning helpers."""

from __future__ import annotations

import pytest
from tools.cq.search.tree_sitter.query.planner import (
    build_pack_plan,
    build_pattern_plan,
    sort_pack_plans,
)

try:
    import tree_sitter_python
    from tree_sitter import Language, Query
except ImportError:  # pragma: no cover - environment dependent
    tree_sitter_python = None
    Language = None
    Query = None

EXPECTED_PATTERN_PLANS = 2


@pytest.mark.skipif(tree_sitter_python is None, reason="tree-sitter-python is unavailable")
def test_build_pattern_plan_orders_by_rooting_and_locality() -> None:
    """Test build pattern plan orders by rooting and locality."""
    assert Language is not None
    assert Query is not None
    assert tree_sitter_python is not None
    language = Language(tree_sitter_python.language())
    query = Query(
        language,
        "(_ (identifier) @non_local)\n(function_definition name: (identifier) @rooted)",
    )

    plans = build_pattern_plan(query)
    assert len(plans) == EXPECTED_PATTERN_PLANS
    assert plans[0].score >= plans[1].score
    assert plans[0].pattern_idx == 1
    assert plans[1].pattern_idx == 0
    assert plans[0].score > plans[1].score


@pytest.mark.skipif(tree_sitter_python is None, reason="tree-sitter-python is unavailable")
def test_build_pack_plan_and_pack_sort() -> None:
    """Test build pack plan and pack sort."""
    assert Language is not None
    assert Query is not None
    assert tree_sitter_python is not None
    language = Language(tree_sitter_python.language())

    query_rooted = Query(language, "(function_definition name: (identifier) @name)")
    query_non_local = Query(language, "(_ (identifier) @name)")

    plan_rooted = build_pack_plan(
        pack_name="rooted",
        query=query_rooted,
        query_text="(function_definition name: (identifier) @name)",
    )
    plan_non_local = build_pack_plan(
        pack_name="non_local",
        query=query_non_local,
        query_text="(_ (identifier) @name)",
    )
    assert plan_rooted.score > plan_non_local.score

    ordered = sort_pack_plans(
        (
            ("non_local", "q2", plan_non_local),
            ("rooted", "q1", plan_rooted),
        ),
    )
    assert ordered[0][0] == "rooted"
    assert ordered[1][0] == "non_local"


def test_build_pattern_plan_handles_invalid_capture_quantifier() -> None:
    """Handle unsupported capture quantifier queries without failing hard."""
    class _FakeQuery:
        pattern_count = 1
        capture_count = 2

        @staticmethod
        def is_pattern_rooted(_pattern_idx: int) -> bool:
            return True

        @staticmethod
        def is_pattern_non_local(_pattern_idx: int) -> bool:
            return False

        @staticmethod
        def is_pattern_guaranteed_at_step(_pattern_idx: int) -> bool:
            return True

        @staticmethod
        def pattern_assertions(_pattern_idx: int) -> dict[str, tuple[str, bool]]:
            return {}

        @staticmethod
        def capture_quantifier(_pattern_idx: int, capture_idx: int) -> str:
            if capture_idx == 0:
                message = "Unexpected capture quantifier of 0"
                raise SystemError(message)
            return "one"

        @staticmethod
        def start_byte_for_pattern(_pattern_idx: int) -> int:
            return 1

        @staticmethod
        def end_byte_for_pattern(_pattern_idx: int) -> int:
            return 10

    plans = build_pattern_plan(_FakeQuery())  # type: ignore[arg-type]
    assert len(plans) == 1
    assert plans[0].capture_quantifiers == ("none", "one")
