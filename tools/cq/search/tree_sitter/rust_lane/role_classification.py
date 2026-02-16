"""Item role classification for Rust nodes.

This module classifies Rust nodes into semantic roles like method,
free_function, test_function, trait_method, method_call, etc.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import _rust_field_ids

if TYPE_CHECKING:
    from tree_sitter import Node


# ---------------------------------------------------------------------------
# Item role classification
# ---------------------------------------------------------------------------

_ITEM_ROLE_SIMPLE: dict[str, str] = {
    "use_declaration": "use_import",
    "macro_invocation": "macro_call",
    "field_declaration": "struct_field",
    "enum_variant": "enum_variant",
    "const_item": "const_item",
    "type_item": "type_alias",
    "static_item": "static_item",
}

_TEST_ATTRIBUTE_NAMES: frozenset[str] = frozenset(
    {
        "test",
        "tokio::test",
        "rstest",
        "async_std::test",
    }
)


def _is_test_function(attributes: list[str]) -> bool:
    """Return whether any attribute marks this function as a test.

    Parameters
    ----------
    attributes
        Attribute strings (already stripped of ``#[...]`` delimiters).

    Returns:
    -------
    bool
        True when at least one attribute matches a known test marker.
    """
    return any(attr in _TEST_ATTRIBUTE_NAMES for attr in attributes)


def _find_ancestor(node: Node, kind: str, *, max_depth: int) -> Node | None:
    """Walk up from *node* to find the nearest ancestor of the given kind.

    Parameters
    ----------
    node
        Starting node.
    kind
        Target node ``type`` string.
    max_depth
        Maximum parent levels to inspect.

    Returns:
    -------
    Node | None
        The ancestor node, or ``None`` if not found within *max_depth*.
    """
    current: Node | None = node.parent
    depth = 0
    while current is not None and depth < max_depth:
        if current.type == kind:
            return current
        current = current.parent
        depth += 1
    return None


def _classify_function_role(
    node: Node,
    attributes: list[str],
    *,
    max_scope_depth: int,
) -> str:
    """Classify a ``function_item`` into a specific role.

    Parameters
    ----------
    node
        A tree-sitter ``function_item`` node.
    attributes
        Pre-extracted attributes for the function.
    max_scope_depth
        Maximum ancestor search depth.

    Returns:
    -------
    str
        One of ``"test_function"``, ``"trait_method"``, ``"method"``,
        or ``"free_function"``.
    """
    if _is_test_function(attributes):
        return "test_function"
    impl_node = _find_ancestor(node, "impl_item", max_depth=max_scope_depth)
    if impl_node is not None:
        trait_node = child_by_field(impl_node, "trait", _rust_field_ids())
        if trait_node is not None:
            return "trait_method"
        return "method"
    return "free_function"


def _classify_call_role(node: Node) -> str:
    """Classify a ``call_expression`` node as method or function call.

    Parameters
    ----------
    node
        A tree-sitter ``call_expression`` node.

    Returns:
    -------
    str
        ``"method_call"`` when the target is a field expression, else
        ``"function_call"``.
    """
    fn_node = child_by_field(node, "function", _rust_field_ids())
    if fn_node is not None and fn_node.type == "field_expression":
        return "method_call"
    return "function_call"


def _classify_item_role(
    node: Node,
    scope: Node | None,
    attributes: list[str],
    *,
    max_scope_depth: int,
) -> str:
    """Classify the item role of a match node.

    Parameters
    ----------
    node
        The tree-sitter node at the match location.
    scope
        The nearest enclosing scope node.
    attributes
        Pre-extracted attribute strings.
    max_scope_depth
        Maximum ancestor search depth.

    Returns:
    -------
    str
        A semantic role string (e.g. ``"method"``, ``"free_function"``).
    """
    simple = _ITEM_ROLE_SIMPLE.get(node.type)
    if simple is not None:
        return simple

    if node.type == "call_expression":
        return _classify_call_role(node)

    fn_target = node if node.type == "function_item" else None
    if fn_target is None and scope is not None and scope.type == "function_item":
        fn_target = scope
    if fn_target is not None:
        return _classify_function_role(fn_target, attributes, max_scope_depth=max_scope_depth)

    return node.type


__all__ = [
    "_classify_item_role",
]
