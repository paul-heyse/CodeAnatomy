"""Tree-sitter injection planning helpers."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from typing import Protocol

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.rust_lane.injection_profiles import (
    resolve_rust_injection_profile,
)
from tools.cq.search.tree_sitter.rust_lane.injection_settings import (
    InjectionSettingsV1,
    settings_for_pattern,
)


class NodeLike(Protocol):
    """Structural node protocol for injection planning."""

    @property
    def start_byte(self) -> int: ...

    @property
    def end_byte(self) -> int: ...

    @property
    def start_point(self) -> tuple[int, int]: ...

    @property
    def end_point(self) -> tuple[int, int]: ...


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
    include_children: bool = False
    use_self_language: bool = False
    use_parent_language: bool = False


class InjectionPlanBuildContextV1(CqStruct, frozen=True):
    """Runtime inputs shared across injection plan rows."""

    source_bytes: bytes
    default_language: str | None


def _node_text(node: NodeLike, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace")


def _resolve_language_name(
    *,
    context: InjectionPlanBuildContextV1,
    settings: InjectionSettingsV1,
    language_nodes: Sequence[NodeLike],
    content_node: NodeLike,
    profile_language: str,
) -> str:
    if settings.language:
        return settings.language

    if language_nodes:
        language_text = _node_text(language_nodes[0], context.source_bytes)
        if language_text:
            return language_text

    if (settings.use_self_language or settings.use_parent_language) and context.default_language:
        return context.default_language

    if profile_language:
        return profile_language

    if context.default_language:
        return context.default_language

    _ = content_node
    return ""


def _plan_from_node(
    *,
    node: NodeLike,
    language: str,
    profile_name: str | None,
    combined: bool,
    include_children: bool,
    use_self_language: bool,
    use_parent_language: bool,
) -> InjectionPlanV1 | None:
    start_byte = int(getattr(node, "start_byte", 0))
    end_byte = int(getattr(node, "end_byte", start_byte))
    if end_byte <= start_byte:
        return None
    start_point = getattr(node, "start_point", (0, 0))
    end_point = getattr(node, "end_point", start_point)
    return InjectionPlanV1(
        language=language,
        start_byte=start_byte,
        end_byte=end_byte,
        start_row=int(start_point[0]),
        start_col=int(start_point[1]),
        end_row=int(end_point[0]),
        end_col=int(end_point[1]),
        profile_name=profile_name,
        combined=combined,
        include_children=include_children,
        use_self_language=use_self_language,
        use_parent_language=use_parent_language,
    )


def build_injection_plan_from_matches(
    *,
    query: object,
    matches: Sequence[tuple[int, Mapping[str, Sequence[NodeLike]]]],
    source_bytes: bytes,
    default_language: str | None = None,
) -> tuple[InjectionPlanV1, ...]:
    """Build deterministic injection plan rows from query ``matches()`` rows.

    Returns:
    -------
    tuple[InjectionPlanV1, ...]
        Ordered embedded-language parse windows.
    """
    context = InjectionPlanBuildContextV1(
        source_bytes=source_bytes,
        default_language=default_language,
    )
    planned: OrderedDict[
        tuple[str, int, int, int, int, int, int, str | None, bool, bool, bool, bool],
        None,
    ] = OrderedDict()

    for pattern_idx, capture_map in matches:
        settings = settings_for_pattern(query, pattern_idx)
        content_nodes = capture_map.get("injection.content", [])
        if not content_nodes:
            continue
        language_nodes = capture_map.get("injection.language", [])
        macro_nodes = capture_map.get("injection.macro.name", [])
        macro_name = ""
        if macro_nodes:
            macro_name = _node_text(macro_nodes[0], source_bytes)
        profile = resolve_rust_injection_profile(macro_name or None)

        for node in content_nodes:
            language = _resolve_language_name(
                context=context,
                settings=settings,
                language_nodes=language_nodes,
                content_node=node,
                profile_language=profile.language,
            )
            if not language:
                continue
            plan = _plan_from_node(
                node=node,
                language=language,
                profile_name=profile.profile_name,
                combined=settings.combined or profile.combined,
                include_children=settings.include_children,
                use_self_language=settings.use_self_language,
                use_parent_language=settings.use_parent_language,
            )
            if plan is None:
                continue
            key = (
                plan.language,
                plan.start_byte,
                plan.end_byte,
                plan.start_row,
                plan.start_col,
                plan.end_row,
                plan.end_col,
                plan.profile_name,
                plan.combined,
                plan.include_children,
                plan.use_self_language,
                plan.use_parent_language,
            )
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
            include_children=include_children,
            use_self_language=use_self_language,
            use_parent_language=use_parent_language,
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
            include_children,
            use_self_language,
            use_parent_language,
        ) in planned
    )


__all__ = [
    "InjectionPlanV1",
    "build_injection_plan_from_matches",
]
