"""Extractor dispatch helpers for Rust lane enrichment payload assembly."""

from __future__ import annotations

import logging
from collections.abc import Callable

from tree_sitter import Node

from tools.cq.search.rust.extractors_shared import find_ancestor
from tools.cq.search.rust.node_access import TreeSitterRustNodeAccess
from tools.cq.search.tree_sitter.contracts.lane_payloads import canonicalize_rust_lane_payload
from tools.cq.search.tree_sitter.rust_lane.enrichment_extractors import (
    extract_attributes_dict,
    extract_call_target,
    extract_enum_shape,
    extract_function_signature,
    extract_impl_context,
    extract_struct_shape,
    extract_visibility_dict,
)
from tools.cq.search.tree_sitter.rust_lane.role_classification import classify_item_role
from tools.cq.search.tree_sitter.rust_lane.scope_utils import (
    _find_scope,
    _scope_chain,
    _scope_name,
)

_DEFINITION_SCOPE_KINDS: frozenset[str] = frozenset(
    {
        "const_item",
        "enum_item",
        "extern_crate_declaration",
        "function_item",
        "impl_item",
        "macro_invocation",
        "mod_item",
        "static_item",
        "struct_item",
        "trait_item",
        "type_item",
    }
)
_CALL_NODE_KINDS: frozenset[str] = frozenset({"call_expression", "macro_invocation"})
_ENRICHMENT_ERRORS = (AttributeError, LookupError, RuntimeError, TypeError, ValueError)
logger = logging.getLogger(__name__)


__all__ = ["_apply_extractors", "_build_enrichment_payload", "_try_extract", "canonicalize_payload"]


def _try_extract(
    label: str,
    extractor: Callable[[Node, bytes], dict[str, object]],
    target: Node,
    source_bytes: bytes,
) -> tuple[dict[str, object], str | None]:
    """Call extractor on target and return fields + optional degrade reason.

    Returns:
        tuple[dict[str, object], str | None]: Extracted fields and degrade reason.
    """
    try:
        result = extractor(target, source_bytes)
    except _ENRICHMENT_ERRORS as exc:
        logger.warning("Rust extractor degraded (%s): %s", label, type(exc).__name__)
        return {}, f"{label}: {exc}"
    return result, None


def _resolve_target(node: Node, scope: Node | None, kind: str) -> Node | None:
    if node.type == kind:
        return node
    if scope is not None and scope.type == kind:
        return scope
    return None


def _resolve_definition_target(node: Node, scope: Node | None) -> Node | None:
    for kind in _DEFINITION_SCOPE_KINDS:
        target = _resolve_target(node, scope, kind)
        if target is not None:
            return target
    return None


def _merge_result(
    payload: dict[str, object],
    reasons: list[str],
    result: tuple[dict[str, object], str | None],
) -> None:
    fields, reason = result
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)


def _resolve_impl_ancestor(
    node: Node,
    scope: Node | None,
    *,
    max_scope_depth: int,
) -> Node | None:
    if scope is not None and scope.type == "impl_item":
        return scope
    ancestor = find_ancestor(
        TreeSitterRustNodeAccess(node, b""),
        "impl_item",
        max_depth=max_scope_depth,
    )
    if isinstance(ancestor, TreeSitterRustNodeAccess):
        return ancestor.node
    return None


def _enrich_visibility_and_attrs(
    payload: dict[str, object],
    reasons: list[str],
    target: Node,
    source_bytes: bytes,
) -> None:
    fields, reason = _try_extract("visibility", extract_visibility_dict, target, source_bytes)
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)

    fields, reason = _try_extract("attributes", extract_attributes_dict, target, source_bytes)
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)


def _apply_extractors(
    payload: dict[str, object],
    node: Node,
    scope: Node | None,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> None:
    """Dispatch all applicable extractors into payload."""
    reasons: list[str] = []

    sig_target = _resolve_target(node, scope, "function_item")
    if sig_target is not None:
        _merge_result(
            payload,
            reasons,
            _try_extract("signature", extract_function_signature, sig_target, source_bytes),
        )

    vis_target = _resolve_definition_target(node, scope)
    if vis_target is not None:
        _enrich_visibility_and_attrs(payload, reasons, vis_target, source_bytes)

    impl_target = _resolve_impl_ancestor(node, scope, max_scope_depth=max_scope_depth)
    if impl_target is not None:
        _merge_result(
            payload,
            reasons,
            _try_extract("impl_context", extract_impl_context, impl_target, source_bytes),
        )

    if scope is not None and scope.type == "struct_item":
        _merge_result(
            payload,
            reasons,
            _try_extract("struct_shape", extract_struct_shape, scope, source_bytes),
        )
    if scope is not None and scope.type == "enum_item":
        _merge_result(
            payload,
            reasons,
            _try_extract("enum_shape", extract_enum_shape, scope, source_bytes),
        )

    if node.type in _CALL_NODE_KINDS:
        _merge_result(
            payload,
            reasons,
            _try_extract("call_target", extract_call_target, node, source_bytes),
        )

    extracted_attrs = payload.get("attributes")
    attr_list: list[str] = extracted_attrs if isinstance(extracted_attrs, list) else []
    try:
        payload["item_role"] = classify_item_role(
            node,
            scope,
            attr_list,
            max_scope_depth=max_scope_depth,
        )
    except _ENRICHMENT_ERRORS as exc:
        logger.warning("Rust item_role classification degraded: %s", type(exc).__name__)
        reasons.append(f"item_role: {exc}")

    if reasons:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = "; ".join(reasons)


def _build_enrichment_payload(
    node: Node,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> dict[str, object]:
    """Build lane payload for one Rust node.

    Returns:
        dict[str, object]: Enrichment payload assembled for the provided node.
    """
    scope = _find_scope(node, max_depth=max_scope_depth)
    payload: dict[str, object] = {
        "node_kind": node.type,
        "scope_chain": _scope_chain(node, source_bytes, max_depth=max_scope_depth),
        "language": "rust",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
    }

    if scope is not None:
        payload["scope_kind"] = scope.type
        scope_name = _scope_name(scope, source_bytes)
        if scope_name:
            payload["scope_name"] = scope_name

    _apply_extractors(payload, node, scope, source_bytes, max_scope_depth=max_scope_depth)
    return payload


def canonicalize_payload(payload: dict[str, object]) -> dict[str, object]:
    """Canonicalize Rust lane payload contract.

    Returns:
        dict[str, object]: Canonicalized Rust lane enrichment payload.
    """
    return canonicalize_rust_lane_payload(payload)
