"""Tree-sitter injection planning helpers."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search._shared.core import node_text as _shared_node_text
from tools.cq.search.tree_sitter.rust_lane.injection_profiles import (
    resolve_rust_injection_profile,
)

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
    profile_name: str | None = None
    combined: bool = False


class InjectionPlanBuildContextV1(CqStruct, frozen=True):
    """Runtime inputs shared across injection plan rows."""

    source_bytes: bytes
    default_language: str | None
    combined_keys: frozenset[tuple[int, int]]


def _node_text(node: Node, source_bytes: bytes) -> str:
    return _shared_node_text(node, source_bytes)


def build_injection_plan(
    source_bytes: bytes,
    *,
    captures: dict[str, list[Node]],
    default_language: str | None = None,
) -> tuple[InjectionPlanV1, ...]:
    """Build deterministic injection plan rows from query captures.

    Returns:
    -------
    tuple[InjectionPlanV1, ...]
        Ordered embedded-language parse windows.
    """
    language_nodes = captures.get("injection.language", [])
    macro_nodes = captures.get("injection.macro.name", [])
    content_nodes = captures.get("injection.content", [])
    combined_nodes = captures.get("injection.combined", [])
    if not content_nodes:
        return ()

    combined_keys = {
        (
            int(getattr(node, "start_byte", 0)),
            int(getattr(node, "end_byte", 0)),
        )
        for node in combined_nodes
    }
    build_context = InjectionPlanBuildContextV1(
        source_bytes=source_bytes,
        default_language=default_language,
        combined_keys=frozenset(combined_keys),
    )
    planned: OrderedDict[tuple[str, int, int, int, int, int, int, str | None, bool], None] = (
        OrderedDict()
    )
    for idx, node in enumerate(content_nodes):
        key = _build_plan_key(
            idx=idx,
            node=node,
            language_nodes=language_nodes,
            macro_nodes=macro_nodes,
            context=build_context,
        )
        if key is not None:
            planned[key] = None

    return tuple(
        InjectionPlanV1(
            language=language,
            start_byte=start_byte,
            end_byte=end_byte,
            start_row=start_row,
            start_col=start_col,
            end_row=end_row,
            end_col=end_col,
            profile_name=profile_name,
            combined=combined,
        )
        for (
            language,
            start_byte,
            end_byte,
            start_row,
            start_col,
            end_row,
            end_col,
            profile_name,
            combined,
        ) in planned
    )


def _build_plan_key(
    *,
    idx: int,
    node: Node,
    language_nodes: list[Node],
    macro_nodes: list[Node],
    context: InjectionPlanBuildContextV1,
) -> tuple[str, int, int, int, int, int, int, str | None, bool] | None:
    language = context.default_language or ""
    profile_name: str | None = None
    combined = False
    macro_name = (
        _node_text(macro_nodes[idx], context.source_bytes) if idx < len(macro_nodes) else ""
    )
    profile = resolve_rust_injection_profile(macro_name or None)
    if profile is not None:
        profile_name = profile.profile_name
        language = profile.language or language
        combined = profile.combined
    if idx < len(language_nodes):
        text = _node_text(language_nodes[idx], context.source_bytes)
        if text:
            language = text
    if not language:
        return None
    start_byte = int(getattr(node, "start_byte", 0))
    end_byte = int(getattr(node, "end_byte", start_byte))
    if end_byte <= start_byte:
        return None
    if (start_byte, end_byte) in context.combined_keys:
        combined = True
    start_point = getattr(node, "start_point", (0, 0))
    end_point = getattr(node, "end_point", start_point)
    return (
        language,
        start_byte,
        end_byte,
        int(start_point[0]),
        int(start_point[1]),
        int(end_point[0]),
        int(end_point[1]),
        profile_name,
        combined,
    )


__all__ = [
    "InjectionPlanV1",
    "build_injection_plan",
]
