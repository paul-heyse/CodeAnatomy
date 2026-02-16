"""Tests for Rust node-access adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from tools.cq.search.rust.node_access import SgRustNodeAccess, TreeSitterRustNodeAccess


@dataclass
class _FakeSgNode:
    node_kind: str
    source_text: str
    fields: dict[str, _FakeSgNode] = field(default_factory=dict)
    named: list[_FakeSgNode] = field(default_factory=list)
    parent_node: _FakeSgNode | None = None

    def kind(self) -> str:
        return self.node_kind

    def text(self) -> str:
        return self.source_text

    def field(self, name: str) -> _FakeSgNode | None:
        return self.fields.get(name)

    def children(self) -> list[_FakeSgNode]:
        return list(self.named)

    @staticmethod
    def is_named() -> bool:
        return True

    def parent(self) -> _FakeSgNode | None:
        return self.parent_node


@dataclass
class _FakeTsNode:
    type: str
    start_byte: int
    end_byte: int
    fields: dict[str, _FakeTsNode] = field(default_factory=dict)
    named_children: list[_FakeTsNode] = field(default_factory=list)
    parent: _FakeTsNode | None = None

    def child_by_field_name(self, name: str) -> _FakeTsNode | None:
        return self.fields.get(name)


def test_sg_node_access_wraps_children_and_parent() -> None:
    """ast-grep adapter exposes kind/text/child/parent semantics."""
    child = _FakeSgNode(node_kind="identifier", source_text="name")
    root = _FakeSgNode(node_kind="function_item", source_text="fn name()", fields={"name": child})
    child.parent_node = root
    root.named.append(child)

    adapter = SgRustNodeAccess(cast("Any", root))
    assert adapter.kind() == "function_item"
    assert adapter.text() == "fn name()"
    wrapped_child = adapter.child_by_field_name("name")
    assert wrapped_child is not None
    assert wrapped_child.kind() == "identifier"
    wrapped_parent = wrapped_child.parent()
    assert wrapped_parent is not None
    assert wrapped_parent.kind() == "function_item"


def test_tree_sitter_node_access_reads_text_and_children() -> None:
    """tree-sitter adapter exposes text spans and wrapped relatives."""
    source = b"fn name() {}"
    child = _FakeTsNode(type="identifier", start_byte=3, end_byte=7)
    root = _FakeTsNode(
        type="function_item", start_byte=0, end_byte=len(source), fields={"name": child}
    )
    child.parent = root
    root.named_children.append(child)

    adapter = TreeSitterRustNodeAccess(cast("Any", root), source)
    assert adapter.kind() == "function_item"
    assert adapter.text() == "fn name() {}"
    wrapped_child = adapter.child_by_field_name("name")
    assert wrapped_child is not None
    assert wrapped_child.text() == "name"
    wrapped_parent = wrapped_child.parent()
    assert wrapped_parent is not None
    assert wrapped_parent.kind() == "function_item"
