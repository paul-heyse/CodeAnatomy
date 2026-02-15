"""Flattened tags: contracts and runtime helpers for tags extraction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

import msgspec

from tools.cq.core.structs import CqStruct
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


class NodeLike(Protocol):
    """Structural node protocol for tag runtime projection."""

    @property
    def start_byte(self) -> int:
        """Return byte offset of node start."""
        ...

    @property
    def end_byte(self) -> int:
        """Return byte offset of node end."""
        ...


def build_tag_events(
    *,
    matches: Sequence[tuple[int, Mapping[str, Sequence[NodeLike]]]],
    source_bytes: bytes,
) -> tuple[RustTagEventV1, ...]:
    """Build normalized tag events from query match rows."""
    rows: list[RustTagEventV1] = []
    for pattern_idx, capture_map in matches:
        kind = "unknown"
        if "role.definition" in capture_map:
            role = "definition"
            kind = "symbol"
        elif "role.reference" in capture_map:
            role = "reference"
            kind = "symbol"
        else:
            continue
        names = capture_map.get("name", [])
        if not names:
            continue
        node = names[0]
        text = node_text(node, source_bytes)
        if not text:
            continue
        rows.append(
            RustTagEventV1(
                role=role,
                kind=kind,
                name=text,
                start_byte=int(getattr(node, "start_byte", 0)),
                end_byte=int(getattr(node, "end_byte", 0)),
                metadata={"pattern_index": pattern_idx},
            )
        )
    return tuple(rows)


__all__ = ["NodeLike", "RustTagEventV1", "build_tag_events"]
