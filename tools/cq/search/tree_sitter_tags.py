"""Tree-sitter tags-style extraction helpers."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tree_sitter import Node


class TagEventV1(CqStruct, frozen=True):
    """One tags-style event row."""

    kind: str
    name: str
    start_byte: int
    end_byte: int


def _node_text(node: Node, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace").strip()


def build_tag_events(
    source_bytes: bytes,
    *,
    captures: dict[str, list[Node]],
) -> tuple[TagEventV1, ...]:
    """Create deterministic tags-style events from capture groups.

    Returns:
    -------
    tuple[TagEventV1, ...]
        Ordered tag events derived from capture groups.
    """
    kinds = {
        "def": ("def.function.name", "def.struct.name", "def.enum.name", "def.trait.name"),
        "ref": ("ref.identifier", "ref.scoped.name", "ref.use.path"),
        "call": ("call.target", "call.macro.path"),
    }
    rows: OrderedDict[tuple[str, str, int, int], None] = OrderedDict()
    for kind, capture_names in kinds.items():
        for capture_name in capture_names:
            for node in captures.get(capture_name, []):
                name = _node_text(node, source_bytes)
                if not name:
                    continue
                start_byte = int(getattr(node, "start_byte", 0))
                end_byte = int(getattr(node, "end_byte", start_byte))
                if end_byte <= start_byte:
                    continue
                rows[kind, name, start_byte, end_byte] = None
    return tuple(
        TagEventV1(kind=kind, name=name, start_byte=start_byte, end_byte=end_byte)
        for kind, name, start_byte, end_byte in rows
    )


__all__ = [
    "TagEventV1",
    "build_tag_events",
]
