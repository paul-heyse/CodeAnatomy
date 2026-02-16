"""Tests for shared Rust extractor helpers."""

from __future__ import annotations

from dataclasses import dataclass, field

from tools.cq.search.rust.extractors_shared import (
    extract_call_target,
    extract_function_signature,
    extract_impl_context,
    extract_visibility,
    find_scope,
    scope_name,
)
from tools.cq.search.rust.node_access import RustNodeAccess


@dataclass
class _Node(RustNodeAccess):
    node_kind: str
    source_text: str
    fields: dict[str, _Node] = field(default_factory=dict)
    named_children: list[_Node] = field(default_factory=list)
    parent_node: _Node | None = None

    def kind(self) -> str:
        return self.node_kind

    def text(self) -> str:
        return self.source_text

    def child_by_field_name(self, name: str) -> _Node | None:
        return self.fields.get(name)

    def children(self) -> list[RustNodeAccess]:
        return list(self.named_children)

    def parent(self) -> _Node | None:
        return self.parent_node


def test_scope_name_for_impl_item_uses_type_field() -> None:
    """Scope-name helper prefers impl type field when present."""
    impl_type = _Node(node_kind="type_identifier", source_text="Engine")
    impl_item = _Node(
        node_kind="impl_item", source_text="impl Engine {}", fields={"type": impl_type}
    )
    assert scope_name(impl_item) == "Engine"


def test_find_scope_walks_parents() -> None:
    """Scope lookup walks parent chain to nearest scope kind."""
    scope = _Node(node_kind="function_item", source_text="fn target() {}")
    leaf = _Node(node_kind="identifier", source_text="target", parent_node=scope)
    assert find_scope(leaf, max_depth=4) is scope


def test_extract_visibility_recognizes_pub_forms() -> None:
    """Visibility extraction recognizes public and private forms."""
    assert (
        extract_visibility(_Node(node_kind="function_item", source_text="pub(crate) fn a() {}"))
        == "pub(crate)"
    )
    assert (
        extract_visibility(_Node(node_kind="function_item", source_text="pub fn a() {}")) == "pub"
    )
    assert (
        extract_visibility(_Node(node_kind="function_item", source_text="fn a() {}")) == "private"
    )


def test_extract_function_signature_collects_fields() -> None:
    """Function signature extractor collects params, return type, and flags."""
    params = _Node(node_kind="parameters", source_text="(a: i32)")
    ret = _Node(node_kind="type_identifier", source_text="i32")
    node = _Node(
        node_kind="function_item",
        source_text="async fn target(a: i32) -> i32 { a }",
        fields={"parameters": params, "return_type": ret},
    )
    params.named_children = [_Node(node_kind="parameter", source_text="a: i32")]

    payload = extract_function_signature(node)
    assert payload["params"] == ["a: i32"]
    assert payload["return_type"] == "i32"
    assert payload["is_async"] is True


def test_extract_call_target_handles_method_calls() -> None:
    """Call-target extractor captures receiver and method for field calls."""
    receiver = _Node(node_kind="identifier", source_text="obj")
    field = _Node(node_kind="field_identifier", source_text="run")
    function = _Node(
        node_kind="field_expression",
        source_text="obj.run",
        fields={"value": receiver, "field": field},
    )
    call = _Node(
        node_kind="call_expression",
        source_text="obj.run()",
        fields={"function": function},
    )
    payload = extract_call_target(call)
    assert payload["call_target"] == "obj.run"
    assert payload["call_receiver"] == "obj"
    assert payload["call_method"] == "run"


def test_extract_impl_context_reports_trait_or_inherent() -> None:
    """Impl-context extractor captures impl type/trait metadata."""
    impl_type = _Node(node_kind="type_identifier", source_text="Engine")
    impl_trait = _Node(node_kind="type_identifier", source_text="Runnable")
    impl_node = _Node(
        node_kind="impl_item",
        source_text="impl Runnable for Engine {}",
        fields={"type": impl_type, "trait": impl_trait},
    )
    payload = extract_impl_context(impl_node)
    assert payload["impl_type"] == "Engine"
    assert payload["impl_trait"] == "Runnable"
    assert payload["impl_kind"] == "trait"
