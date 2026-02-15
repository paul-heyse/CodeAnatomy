"""Query specialization helpers using tree-sitter disable APIs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, overload

from tools.cq.search.tree_sitter.contracts.query_models import (
    QuerySpecializationProfileV1,
)

if TYPE_CHECKING:
    from tree_sitter import Query


class SupportsQuerySpecialization(Protocol):
    """Structural query contract needed for specialization."""

    pattern_count: int
    capture_count: int

    def disable_pattern(self, pattern_idx: int) -> None: ...

    def disable_capture(self, capture_name: str) -> None: ...

    def capture_name(self, capture_idx: int) -> str: ...

    def pattern_settings(self, pattern_idx: int) -> Mapping[str, object]: ...


_PROFILES: dict[str, QuerySpecializationProfileV1] = {
    "artifact": QuerySpecializationProfileV1(request_surface="artifact"),
    "diagnostic": QuerySpecializationProfileV1(
        request_surface="diagnostic",
        disabled_capture_prefixes=("payload.",),
    ),
    "terminal": QuerySpecializationProfileV1(
        request_surface="terminal",
        disabled_pattern_settings=("cq.surface=artifact_only",),
        disabled_capture_prefixes=("payload.", "debug."),
    ),
}


def _profile_for_surface(request_surface: str) -> QuerySpecializationProfileV1:
    if request_surface in _PROFILES:
        return _PROFILES[request_surface]
    return QuerySpecializationProfileV1(request_surface=request_surface)


def _pattern_setting_values(
    query: Query | SupportsQuerySpecialization, pattern_idx: int
) -> set[str]:
    """Collect normalized query pattern settings for one pattern.

    Returns:
        set[str]: Profile setting strings for the pattern.
    """
    settings = query.pattern_settings(pattern_idx)
    if not isinstance(settings, Mapping):
        return set()
    values: set[str] = set()
    for key, value in settings.items():
        if not isinstance(key, str):
            continue
        if value is None:
            continue
        values.add(f"{key}={value}")
    return values


def _disable_patterns(
    query: Query | SupportsQuerySpecialization,
    profile: QuerySpecializationProfileV1,
) -> None:
    """Disable patterns excluded by a specialization profile."""
    pattern_count = int(getattr(query, "pattern_count", 0))
    if pattern_count <= 0 or not profile.disabled_pattern_settings:
        return
    for pattern_idx in range(pattern_count):
        settings = _pattern_setting_values(query, pattern_idx)
        if settings.intersection(profile.disabled_pattern_settings):
            query.disable_pattern(pattern_idx)


def _disable_captures(
    query: Query | SupportsQuerySpecialization,
    profile: QuerySpecializationProfileV1,
) -> None:
    """Disable captures excluded by a specialization profile."""
    capture_count = int(getattr(query, "capture_count", 0))
    if capture_count <= 0:
        return
    for capture_idx in range(capture_count):
        capture_name = str(query.capture_name(capture_idx))
        if capture_name in profile.disabled_capture_names:
            query.disable_capture(capture_name)
            continue
        if any(capture_name.startswith(prefix) for prefix in profile.disabled_capture_prefixes):
            query.disable_capture(capture_name)


@overload
def specialize_query(query: Query, *, request_surface: str = "artifact") -> Query: ...


@overload
def specialize_query(
    query: SupportsQuerySpecialization,
    *,
    request_surface: str = "artifact",
) -> SupportsQuerySpecialization: ...


def specialize_query(
    query: Query | SupportsQuerySpecialization,
    *,
    request_surface: str = "artifact",
) -> Query | SupportsQuerySpecialization:
    """Apply request-surface specialization in-place and return the query.

    Returns:
        Query | SupportsQuerySpecialization: Query with disabled patterns and captures applied.
    """
    profile = _profile_for_surface(request_surface)
    _disable_patterns(query, profile)
    _disable_captures(query, profile)
    return query


__all__ = ["specialize_query"]
