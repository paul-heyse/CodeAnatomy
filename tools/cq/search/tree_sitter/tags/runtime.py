"""Runtime helpers for Rust tags query projections."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from tools.cq.search.tree_sitter.tags.contracts import RustTagEventV1


class NodeLike(Protocol):
    """Structural node protocol for tag runtime projection."""

    @property
    def start_byte(self) -> int: ...

    @property
    def end_byte(self) -> int: ...


def _node_text(node: NodeLike, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace")


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
        text = _node_text(node, source_bytes)
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


__all__ = ["build_tag_events"]
