"""Enrichment field extractors for Rust nodes.

This module contains extractors for function signatures, visibility,
attributes, impl contexts, call targets, and struct/enum shapes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search._shared.core import truncate as _shared_truncate
from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import _rust_field_ids

if TYPE_CHECKING:
    from tree_sitter import Node


# ---------------------------------------------------------------------------
# Payload bounds constants
# ---------------------------------------------------------------------------

_MAX_SIGNATURE_LEN = 200
_MAX_PARAMS = 12
_MAX_RETURN_TYPE_LEN = 100
_MAX_GENERICS_LEN = 100
_MAX_ATTRIBUTES = 10
_MAX_ATTRIBUTE_LEN = 60
_MAX_FIELDS_SHOWN = 8
_MAX_VARIANTS_SHOWN = 12
_MAX_MEMBER_TEXT_LEN = 60
_MAX_CALL_TARGET_LEN = 120
_MAX_CALL_RECEIVER_LEN = 80


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, max_len: int) -> str:
    """Truncate *text* to *max_len* characters, appending ``...`` when needed.

    Parameters
    ----------
    text
        The string to truncate.
    max_len
        Maximum allowed length (must be >= 4 for the ellipsis to fit).

    Returns:
    -------
    str
        The original or truncated string.
    """
    return _shared_truncate(text, max_len)


def _optional_field_text(
    parent: Node,
    field_name: str,
    source_bytes: bytes,
    *,
    max_len: int,
) -> str | None:
    """Extract and truncate text from an optional named field child.

    Parameters
    ----------
    parent
        Parent node to query.
    field_name
        Field name to look up via ``child_by_field_name``.
    source_bytes
        Full source bytes.
    max_len
        Truncation limit for the extracted text.

    Returns:
    -------
    str | None
        Truncated text, or ``None`` if the field is absent or empty.
    """
    child = child_by_field(parent, field_name, _rust_field_ids())
    text = node_text(child, source_bytes)
    if text is None:
        return None
    return _truncate(text, max_len)


# ---------------------------------------------------------------------------
# Core enrichment extractors
# ---------------------------------------------------------------------------

_MODIFIER_STOP_KINDS: frozenset[str] = frozenset({"identifier", "parameters", "type_parameters"})


def _extract_fn_params(fn_node: Node, source_bytes: bytes) -> list[str]:
    """Extract parameter list from a function node.

    Parameters
    ----------
    fn_node
        A tree-sitter ``function_item`` node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    list[str]
        Parameter text values, capped at ``_MAX_PARAMS``.
    """
    params_node = child_by_field(fn_node, "parameters", _rust_field_ids())
    if params_node is None:
        return []
    param_list: list[str] = []
    for child in params_node.named_children:
        text = node_text(child, source_bytes)
        if text is not None:
            param_list.append(_truncate(text, _MAX_MEMBER_TEXT_LEN))
        if len(param_list) >= _MAX_PARAMS:
            break
    return param_list


def _extract_fn_modifiers(fn_node: Node, source_bytes: bytes) -> tuple[bool, bool]:
    """Detect ``async`` and ``unsafe`` modifiers on a function node.

    Parameters
    ----------
    fn_node
        A tree-sitter ``function_item`` node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    tuple[bool, bool]
        ``(is_async, is_unsafe)`` flags.
    """
    is_async = False
    is_unsafe = False
    for child in fn_node.children:
        if child.type == "mutable_specifier":
            continue
        child_text = node_text(child, source_bytes)
        if child_text == "async":
            is_async = True
        elif child_text == "unsafe":
            is_unsafe = True
        if child.type in _MODIFIER_STOP_KINDS:
            break
    return is_async, is_unsafe


def _extract_function_signature(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract function signature details from a ``function_item`` node.

    Parameters
    ----------
    fn_node
        A tree-sitter node of type ``function_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Signature fields: params, return_type, generics, signature, is_async,
        is_unsafe.
    """
    result: dict[str, object] = {}
    result["params"] = _extract_fn_params(fn_node, source_bytes)

    ret_text = _optional_field_text(
        fn_node, "return_type", source_bytes, max_len=_MAX_RETURN_TYPE_LEN
    )
    if ret_text is not None:
        result["return_type"] = ret_text

    gen_text = _optional_field_text(
        fn_node, "type_parameters", source_bytes, max_len=_MAX_GENERICS_LEN
    )
    if gen_text is not None:
        result["generics"] = gen_text

    body_node = child_by_field(fn_node, "body", _rust_field_ids())
    if body_node is not None:
        sig_bytes = source_bytes[fn_node.start_byte : body_node.start_byte]
        sig_text = sig_bytes.decode("utf-8", errors="replace").strip()
        if sig_text:
            result["signature"] = _truncate(sig_text, _MAX_SIGNATURE_LEN)

    is_async, is_unsafe = _extract_fn_modifiers(fn_node, source_bytes)
    result["is_async"] = is_async
    result["is_unsafe"] = is_unsafe
    return result


def _extract_visibility(item_node: Node, source_bytes: bytes) -> str:
    """Extract visibility qualifier from a definition node.

    Parameters
    ----------
    item_node
        A tree-sitter definition node (function_item, struct_item, etc.).
    source_bytes
        Full source bytes.

    Returns:
    -------
    str
        One of ``"pub"``, ``"pub(crate)"``, ``"pub(super)"``, or ``"private"``.
    """
    vis_node = child_by_field(item_node, "visibility_modifier", _rust_field_ids())
    if vis_node is None:
        for child in item_node.children:
            if child.type == "visibility_modifier":
                vis_node = child
                break
    if vis_node is None:
        return "private"

    vis_text = node_text(vis_node, source_bytes)
    if vis_text is None:
        return "private"

    normalized = vis_text.replace(" ", "")
    if normalized == "pub(crate)":
        return "pub(crate)"
    if normalized == "pub(super)":
        return "pub(super)"
    if normalized.startswith("pub"):
        return "pub"
    return "private"


_COMMENT_KINDS: frozenset[str] = frozenset({"line_comment", "block_comment"})


def _extract_attributes(item_node: Node, source_bytes: bytes) -> list[str]:
    """Extract attribute annotations preceding an item node.

    Walk the ``prev_named_sibling`` chain backwards, collecting
    ``attribute_item`` nodes and skipping comment nodes. Stop at any
    other node type.

    Parameters
    ----------
    item_node
        The definition node whose attributes to collect.
    source_bytes
        Full source bytes.

    Returns:
    -------
    list[str]
        Attribute text values in declaration order, stripped of ``#[...]``
        delimiters. Inner attributes (``#![...]``) are excluded.
    """
    attrs: list[str] = []
    current: Node | None = item_node.prev_named_sibling
    while current is not None:
        if current.type == "attribute_item":
            text = node_text(current, source_bytes)
            if text is not None:
                if text.startswith("#!["):
                    current = current.prev_named_sibling
                    continue
                stripped = text
                if stripped.startswith("#[") and stripped.endswith("]"):
                    stripped = stripped[2:-1]
                attrs.append(_truncate(stripped, _MAX_ATTRIBUTE_LEN))
        elif current.type in _COMMENT_KINDS:
            current = current.prev_named_sibling
            continue
        else:
            break
        current = current.prev_named_sibling

    attrs.reverse()
    return attrs[:_MAX_ATTRIBUTES]


def _extract_impl_context(impl_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract context from an ``impl_item`` node.

    Parameters
    ----------
    impl_node
        A tree-sitter node of type ``impl_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: impl_type, impl_trait (optional), impl_kind, impl_generics
        (optional).
    """
    result: dict[str, object] = {}

    type_text = _optional_field_text(impl_node, "type", source_bytes, max_len=_MAX_CALL_TARGET_LEN)
    if type_text is not None:
        result["impl_type"] = type_text

    trait_node = child_by_field(impl_node, "trait", _rust_field_ids())
    if trait_node is not None:
        trait_text = node_text(trait_node, source_bytes)
        if trait_text is not None:
            result["impl_trait"] = trait_text
        result["impl_kind"] = "trait"
    else:
        result["impl_kind"] = "inherent"

    gen_text = _optional_field_text(
        impl_node, "type_parameters", source_bytes, max_len=_MAX_GENERICS_LEN
    )
    if gen_text is not None:
        result["impl_generics"] = gen_text

    return result


def _extract_field_expression(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Decompose a ``field_expression`` call target into receiver and method.

    Parameters
    ----------
    fn_node
        A tree-sitter ``field_expression`` node (the ``function`` child
        of a ``call_expression``).
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: call_receiver and call_method (when extractable).
    """
    result: dict[str, object] = {}
    recv_text = _optional_field_text(fn_node, "value", source_bytes, max_len=_MAX_CALL_RECEIVER_LEN)
    if recv_text is not None:
        result["call_receiver"] = recv_text
    method_text = node_text(child_by_field(fn_node, "field", _rust_field_ids()), source_bytes)
    if method_text is not None:
        result["call_method"] = method_text
    return result


def _extract_call_target(node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract call target information from a call or macro invocation node.

    Parameters
    ----------
    node
        A tree-sitter node of type ``call_expression`` or ``macro_invocation``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: call_target (and optionally call_receiver, call_method) for
        calls; macro_name for macro invocations.
    """
    result: dict[str, object] = {}

    if node.type == "call_expression":
        fn_node = child_by_field(node, "function", _rust_field_ids())
        if fn_node is not None:
            target_text = node_text(fn_node, source_bytes)
            if target_text is not None:
                result["call_target"] = _truncate(target_text, _MAX_CALL_TARGET_LEN)
            if fn_node.type == "field_expression":
                result.update(_extract_field_expression(fn_node, source_bytes))
    elif node.type == "macro_invocation":
        macro_text = node_text(child_by_field(node, "macro", _rust_field_ids()), source_bytes)
        if macro_text is not None:
            result["macro_name"] = macro_text

    return result


def _extract_struct_shape(struct_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract field shape from a ``struct_item`` node.

    Parameters
    ----------
    struct_node
        A tree-sitter node of type ``struct_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: struct_field_count, struct_fields (list of field text, capped).
    """
    result: dict[str, object] = {}
    body = child_by_field(struct_node, "body", _rust_field_ids())
    if body is None:
        return result

    field_nodes = [c for c in body.named_children if c.type == "field_declaration"]
    result["struct_field_count"] = len(field_nodes)

    fields: list[str] = []
    for field_node in field_nodes[:_MAX_FIELDS_SHOWN]:
        text = node_text(field_node, source_bytes)
        if text is not None:
            fields.append(_truncate(text, _MAX_MEMBER_TEXT_LEN))

    remaining = len(field_nodes) - _MAX_FIELDS_SHOWN
    if remaining > 0:
        fields.append(f"... and {remaining} more")
    result["struct_fields"] = fields
    return result


def _extract_enum_shape(enum_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract variant shape from an ``enum_item`` node.

    Parameters
    ----------
    enum_node
        A tree-sitter node of type ``enum_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: enum_variant_count, enum_variants (list of variant text, capped).
    """
    result: dict[str, object] = {}
    body = child_by_field(enum_node, "body", _rust_field_ids())
    if body is None:
        return result

    variant_nodes = [c for c in body.named_children if c.type == "enum_variant"]
    result["enum_variant_count"] = len(variant_nodes)

    variants: list[str] = []
    for variant_node in variant_nodes[:_MAX_VARIANTS_SHOWN]:
        text = node_text(variant_node, source_bytes)
        if text is not None:
            variants.append(_truncate(text, _MAX_MEMBER_TEXT_LEN))

    remaining = len(variant_nodes) - _MAX_VARIANTS_SHOWN
    if remaining > 0:
        variants.append(f"... and {remaining} more")
    result["enum_variants"] = variants
    return result


def _extract_visibility_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    """Wrap ``_extract_visibility`` to return a dict for ``_try_extract``.

    Parameters
    ----------
    target
        Definition node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Single-key dict with ``visibility``.
    """
    return {"visibility": _extract_visibility(target, source_bytes)}


def _extract_attributes_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    """Wrap ``_extract_attributes`` to return a dict for ``_try_extract``.

    Parameters
    ----------
    target
        Definition node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Dict with ``attributes`` key if any attributes found, else empty.
    """
    attrs = _extract_attributes(target, source_bytes)
    if attrs:
        return {"attributes": attrs}
    return {}


__all__ = [
    "_extract_attributes_dict",
    "_extract_call_target",
    "_extract_enum_shape",
    "_extract_function_signature",
    "_extract_impl_context",
    "_extract_struct_shape",
    "_extract_visibility_dict",
]
