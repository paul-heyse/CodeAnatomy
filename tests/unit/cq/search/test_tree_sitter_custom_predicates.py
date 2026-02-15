"""Unit tests for custom tree-sitter query predicate callbacks."""

from __future__ import annotations

from tools.cq.search.tree_sitter_custom_predicates import make_query_predicate


class _Node:
    def __init__(self, text: str) -> None:
        self.text = text.encode("utf-8")


def test_cq_eq_predicate_true_when_capture_text_matches_literal() -> None:
    predicate = make_query_predicate()
    ok = predicate(
        "cq-eq?",
        [("capture", "cap"), ("literal", "target")],
        0,
        {"cap": [_Node("target")]},
    )
    assert ok is True


def test_cq_match_predicate_evaluates_regex() -> None:
    predicate = make_query_predicate()
    ok = predicate(
        "cq-match?",
        [("capture", "cap"), ("literal", r"^foo_[0-9]+$")],
        0,
        {"cap": [_Node("foo_7")]},
    )
    assert ok is True


def test_cq_any_of_predicate_checks_membership() -> None:
    predicate = make_query_predicate()
    ok = predicate(
        "cq-any-of?",
        [("capture", "cap"), ("literal", "a"), ("literal", "b"), ("literal", "x")],
        0,
        {"cap": [_Node("x")]},
    )
    assert ok is True


def test_cq_regex_predicate_uses_capture_name() -> None:
    predicate = make_query_predicate()
    ok = predicate(
        "cq-regex?",
        [("capture", "cap"), ("literal", "stable_[a-z]+")],
        0,
        {"cap": [_Node("stable_id")]},
    )
    assert ok is True
