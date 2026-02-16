"""Scope-aware locals indexing helpers for Python tree-sitter captures."""

from __future__ import annotations

from collections.abc import Sequence

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import NodeLike
from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.python_lane.runtime import get_python_field_ids
from tools.cq.search.tree_sitter.core.node_utils import node_text


class LocalBindingV1(CqStruct, frozen=True):
    """One local-binding row linked to an inferred scope span."""

    name: str
    scope_start: int
    scope_end: int
    definition_start: int


from tools.cq.search.tree_sitter.python_lane.runtime import get_python_field_ids


def _contains(container: NodeLike, item: NodeLike) -> bool:
    container_start = int(getattr(container, "start_byte", 0))
    container_end = int(getattr(container, "end_byte", container_start))
    item_start = int(getattr(item, "start_byte", 0))
    item_end = int(getattr(item, "end_byte", item_start))
    return container_start <= item_start and container_end >= item_end


def _nearest_scope(node: NodeLike, scopes: Sequence[NodeLike]) -> NodeLike | None:
    nearest: NodeLike | None = None
    nearest_width = 1 << 62
    for scope in scopes:
        if not _contains(scope, node):
            continue
        width = int(getattr(scope, "end_byte", 0)) - int(getattr(scope, "start_byte", 0))
        if width < nearest_width:
            nearest = scope
            nearest_width = width
    return nearest


def _scope_label(scope: NodeLike, source_bytes: bytes) -> str:
    name_node = child_by_field(scope, "name", get_python_field_ids())
    if name_node is not None:
        name = node_text(name_node, source_bytes)
        if name:
            return name
    return str(getattr(scope, "type", "scope"))


def build_locals_index(
    *,
    definitions: Sequence[NodeLike],
    scopes: Sequence[NodeLike],
    source_bytes: bytes,
) -> tuple[LocalBindingV1, ...]:
    """Build typed local-binding rows anchored to nearest local scope.

    Returns:
        tuple[LocalBindingV1, ...]: Function return value.
    """
    bindings: list[LocalBindingV1] = []
    for node in definitions:
        name = node_text(node, source_bytes)
        if not name:
            continue
        scope = _nearest_scope(node, scopes)
        if scope is None:
            scope_start = int(getattr(node, "start_byte", 0))
            scope_end = int(getattr(node, "end_byte", scope_start))
        else:
            scope_start = int(getattr(scope, "start_byte", 0))
            scope_end = int(getattr(scope, "end_byte", scope_start))
        bindings.append(
            LocalBindingV1(
                name=name,
                scope_start=scope_start,
                scope_end=scope_end,
                definition_start=int(getattr(node, "start_byte", 0)),
            )
        )
    return tuple(bindings)


def scope_chain_for_anchor(
    *,
    anchor: NodeLike,
    scopes: Sequence[NodeLike],
    source_bytes: bytes,
) -> list[str]:
    """Build deterministic scope-name chain from available ``@local.scope`` rows.

    Returns:
        list[str]: Function return value.
    """
    anchor_start = int(getattr(anchor, "start_byte", 0))
    anchor_end = int(getattr(anchor, "end_byte", anchor_start))
    containing: list[NodeLike] = []
    for scope in scopes:
        scope_start = int(getattr(scope, "start_byte", 0))
        scope_end = int(getattr(scope, "end_byte", scope_start))
        if scope_start <= anchor_start and scope_end >= anchor_end:
            containing.append(scope)
    if not containing:
        return ["<module>"]
    containing.sort(
        key=lambda scope: (
            int(getattr(scope, "start_byte", 0)),
            -(int(getattr(scope, "end_byte", 0)) - int(getattr(scope, "start_byte", 0))),
        )
    )
    chain = [_scope_label(scope, source_bytes) for scope in containing]
    return chain if chain else ["<module>"]


__all__ = ["LocalBindingV1", "build_locals_index", "scope_chain_for_anchor"]
