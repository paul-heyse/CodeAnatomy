"""Flattened tags: contracts and runtime helpers for tags extraction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.contracts.core_models import NodeLike
from tools.cq.search.tree_sitter.core.node_utils import node_text

# -- Contracts ----------------------------------------------------------------


class RustTagEventV1(CqStruct, frozen=True):
    """One Rust tag event row derived from tags query captures."""

    role: str
    kind: str
    name: str
    start_byte: int
    end_byte: int
    metadata: dict[str, object] = msgspec.field(default_factory=dict)


# -- Runtime ------------------------------------------------------------------


def _normalized_role(capture_names: Sequence[str]) -> str | None:
    for capture_name in capture_names:
        if capture_name.startswith(("role.definition", "definition.")):
            return "definition"
        if capture_name.startswith(("role.reference", "reference.")):
            return "reference"
    return None


def _name_nodes(capture_map: Mapping[str, Sequence[NodeLike]]) -> Sequence[NodeLike]:
    names = capture_map.get("name", ())
    if names:
        return names
    for capture_name, nodes in capture_map.items():
        if capture_name.endswith(".name") and nodes:
            return nodes
    return ()


def build_tag_events(
    *,
    matches: Sequence[tuple[int, Mapping[str, Sequence[NodeLike]]]],
    source_bytes: bytes,
) -> tuple[RustTagEventV1, ...]:
    """Build normalized tag events from query match rows.

    Returns:
        tuple[RustTagEventV1, ...]: Function return value.
    """
    rows: list[RustTagEventV1] = []
    for pattern_idx, capture_map in matches:
        role = _normalized_role(tuple(capture_map.keys()))
        if role is None:
            continue
        names = _name_nodes(capture_map)
        if not names:
            continue
        node = names[0]
        text = node_text(node, source_bytes)
        if not text:
            continue
        rows.append(
            RustTagEventV1(
                role=role,
                kind="symbol",
                name=text,
                start_byte=int(getattr(node, "start_byte", 0)),
                end_byte=int(getattr(node, "end_byte", 0)),
                metadata={"pattern_index": pattern_idx},
            )
        )
    return tuple(rows)


__all__ = ["NodeLike", "RustTagEventV1", "build_tag_events"]
