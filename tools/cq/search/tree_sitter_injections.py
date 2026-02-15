"""Tree-sitter injection planning helpers."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tree_sitter import Node


class InjectionPlanV1(CqStruct, frozen=True):
    """One embedded-language parse plan entry."""

    language: str
    start_byte: int
    end_byte: int
    start_row: int = 0
    start_col: int = 0
    end_row: int = 0
    end_col: int = 0


def _node_text(node: Node, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace").strip()


def build_injection_plan(
    source_bytes: bytes,
    *,
    captures: dict[str, list[Node]],
    default_language: str = "rust",
) -> tuple[InjectionPlanV1, ...]:
    """Build deterministic injection plan rows from query captures.

    Returns:
    -------
    tuple[InjectionPlanV1, ...]
        Ordered embedded-language parse windows.
    """
    language_nodes = captures.get("injection.language", [])
    content_nodes = captures.get("injection.content", [])
    if not content_nodes:
        return ()

    planned: OrderedDict[tuple[str, int, int, int, int, int, int], None] = OrderedDict()
    for idx, node in enumerate(content_nodes):
        language = default_language
        if idx < len(language_nodes):
            text = _node_text(language_nodes[idx], source_bytes)
            if text:
                language = text
        start_byte = int(getattr(node, "start_byte", 0))
        end_byte = int(getattr(node, "end_byte", start_byte))
        if end_byte <= start_byte:
            continue
        start_point = getattr(node, "start_point", (0, 0))
        end_point = getattr(node, "end_point", start_point)
        planned[
            language,
            start_byte,
            end_byte,
            int(start_point[0]),
            int(start_point[1]),
            int(end_point[0]),
            int(end_point[1]),
        ] = None

    return tuple(
        InjectionPlanV1(
            language=language,
            start_byte=start_byte,
            end_byte=end_byte,
            start_row=start_row,
            start_col=start_col,
            end_row=end_row,
            end_col=end_col,
        )
        for language, start_byte, end_byte, start_row, start_col, end_row, end_col in planned
    )


__all__ = [
    "InjectionPlanV1",
    "build_injection_plan",
]
