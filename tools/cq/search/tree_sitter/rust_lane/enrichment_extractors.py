"""Enrichment field extractors for Rust tree-sitter nodes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.rust.extractors_shared import (
    extract_attributes as _extract_attributes_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_call_target as _extract_call_target_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_enum_shape as _extract_enum_shape_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_function_signature as _extract_function_signature_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_impl_context as _extract_impl_context_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_struct_shape as _extract_struct_shape_shared,
)
from tools.cq.search.rust.extractors_shared import (
    extract_visibility as _extract_visibility_shared,
)
from tools.cq.search.rust.node_access import TreeSitterRustNodeAccess

if TYPE_CHECKING:
    from tree_sitter import Node

_MAX_FIELDS_SHOWN = 8
_MAX_VARIANTS_SHOWN = 12


def _extract_function_signature(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    return _extract_function_signature_shared(TreeSitterRustNodeAccess(fn_node, source_bytes))


def _extract_visibility(item_node: Node, source_bytes: bytes) -> str:
    return _extract_visibility_shared(TreeSitterRustNodeAccess(item_node, source_bytes))


def _extract_attributes(item_node: Node, source_bytes: bytes) -> list[str]:
    return _extract_attributes_shared(TreeSitterRustNodeAccess(item_node, source_bytes))


def _extract_impl_context(impl_node: Node, source_bytes: bytes) -> dict[str, object]:
    return _extract_impl_context_shared(TreeSitterRustNodeAccess(impl_node, source_bytes))


def _extract_call_target(node: Node, source_bytes: bytes) -> dict[str, object]:
    return _extract_call_target_shared(TreeSitterRustNodeAccess(node, source_bytes))


def _extract_struct_shape(struct_node: Node, source_bytes: bytes) -> dict[str, object]:
    return _extract_struct_shape_shared(TreeSitterRustNodeAccess(struct_node, source_bytes))


def _extract_enum_shape(enum_node: Node, source_bytes: bytes) -> dict[str, object]:
    return _extract_enum_shape_shared(TreeSitterRustNodeAccess(enum_node, source_bytes))


def _extract_visibility_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    return {"visibility": _extract_visibility(target, source_bytes)}


def _extract_attributes_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    attrs = _extract_attributes(target, source_bytes)
    if attrs:
        return {"attributes": attrs}
    return {}


__all__ = [
    "_MAX_FIELDS_SHOWN",
    "_MAX_VARIANTS_SHOWN",
    "_extract_attributes_dict",
    "_extract_call_target",
    "_extract_enum_shape",
    "_extract_function_signature",
    "_extract_impl_context",
    "_extract_struct_shape",
    "_extract_visibility_dict",
]
