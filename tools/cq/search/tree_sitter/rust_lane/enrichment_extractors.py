"""Enrichment field extractors for Rust tree-sitter nodes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.rust.extractors_shared import (
    extract_attributes as extract_attributes_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_call_target as extract_call_target_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_enum_shape as extract_enum_shape_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_function_signature as extract_function_signature_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_impl_context as extract_impl_context_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_struct_shape as extract_struct_shape_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_visibility as extract_visibility_shared,
)
from tools.cq.search.rust.node_access import TreeSitterRustNodeAccess

if TYPE_CHECKING:
    from tree_sitter import Node

_MAX_FIELDS_SHOWN = 8
_MAX_VARIANTS_SHOWN = 12


def extract_function_signature(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract normalized Rust function signature metadata.

    Returns:
    -------
    dict[str, object]
        Normalized signature payload.
    """
    return extract_function_signature_shared(TreeSitterRustNodeAccess(fn_node, source_bytes))


def extract_visibility(item_node: Node, source_bytes: bytes) -> str:
    """Extract normalized Rust visibility modifier.

    Returns:
    -------
    str
        Visibility token for the item.
    """
    return extract_visibility_shared(TreeSitterRustNodeAccess(item_node, source_bytes))


def extract_attributes(item_node: Node, source_bytes: bytes) -> list[str]:
    """Extract normalized Rust attributes.

    Returns:
    -------
    list[str]
        Attribute text values attached to the item.
    """
    return extract_attributes_shared(TreeSitterRustNodeAccess(item_node, source_bytes))


def extract_impl_context(impl_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract implementation context metadata for an impl block.

    Returns:
    -------
    dict[str, object]
        Normalized impl context payload.
    """
    return extract_impl_context_shared(TreeSitterRustNodeAccess(impl_node, source_bytes))


def extract_call_target(node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract normalized call target metadata from a call expression node.

    Returns:
    -------
    dict[str, object]
        Normalized call target payload.
    """
    return extract_call_target_shared(TreeSitterRustNodeAccess(node, source_bytes))


def extract_struct_shape(struct_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract normalized struct shape metadata.

    Returns:
    -------
    dict[str, object]
        Normalized struct shape payload.
    """
    return extract_struct_shape_shared(TreeSitterRustNodeAccess(struct_node, source_bytes))


def extract_enum_shape(enum_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract normalized enum shape metadata.

    Returns:
    -------
    dict[str, object]
        Normalized enum shape payload.
    """
    return extract_enum_shape_shared(TreeSitterRustNodeAccess(enum_node, source_bytes))


def extract_visibility_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    """Return visibility metadata wrapped in a dictionary payload."""
    return {"visibility": extract_visibility(target, source_bytes)}


def extract_attributes_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    """Return attributes metadata wrapped in a dictionary payload."""
    attrs = extract_attributes(target, source_bytes)
    if attrs:
        return {"attributes": attrs}
    return {}


__all__ = [
    "_MAX_FIELDS_SHOWN",
    "_MAX_VARIANTS_SHOWN",
    "extract_attributes",
    "extract_attributes_dict",
    "extract_call_target",
    "extract_enum_shape",
    "extract_function_signature",
    "extract_impl_context",
    "extract_struct_shape",
    "extract_visibility",
    "extract_visibility_dict",
]
