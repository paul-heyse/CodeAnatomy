"""Unit tests for custom tree-sitter query predicate callbacks."""

from __future__ import annotations

from tools.cq.search.tree_sitter.query.predicates import make_query_predicate


class _Node:
    def __init__(self, text: str) -> None:
        self.text = text.encode("utf-8")


def test_cq_eq_predicate_true_when_capture_text_matches_literal() -> None:
    """Test cq eq predicate true when capture text matches literal."""
    predicate = make_query_predicate()
    ok = predicate(
        "cq-eq?",
        [("capture", "cap"), ("literal", "target")],
        0,
        {"cap": [_Node("target")]},
    )
    assert ok is True


def test_cq_match_predicate_evaluates_regex() -> None:
    """Test cq match predicate evaluates regex."""
    predicate = make_query_predicate()
    ok = predicate(
        "cq-match?",
        [("capture", "cap"), ("literal", r"^foo_[0-9]+$")],
        0,
        {"cap": [_Node("foo_7")]},
    )
    assert ok is True


def test_cq_any_of_predicate_checks_membership() -> None:
    """Test cq any of predicate checks membership."""
    predicate = make_query_predicate()
    ok = predicate(
        "cq-any-of?",
        [("capture", "cap"), ("literal", "a"), ("literal", "b"), ("literal", "x")],
        0,
        {"cap": [_Node("x")]},
    )
    assert ok is True


def test_cq_regex_predicate_uses_capture_name() -> None:
    """Test cq regex predicate uses capture name."""
    predicate = make_query_predicate()
    ok = predicate(
        "cq-regex?",
        [("capture", "cap"), ("literal", "stable_[a-z]+")],
        0,
        {"cap": [_Node("stable_id")]},
    )
    assert ok is True
