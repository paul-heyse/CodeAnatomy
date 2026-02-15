"""Tests for shared tree-sitter node text helpers."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.core.text_utils import node_text


@dataclass(frozen=True)
class _Node:
    start_byte: int
    end_byte: int


def test_node_text_decodes_and_truncates() -> None:
    source = b"  abcdefgh  "
    node = _Node(start_byte=0, end_byte=len(source))
    assert node_text(node, source, max_len=8) == "abcde..."


def test_node_text_returns_empty_for_invalid_span() -> None:
    source = b"abc"
    node = _Node(start_byte=2, end_byte=2)
    assert node_text(node, source) == ""
