"""Typed option structs for cq CLI commands."""

from __future__ import annotations

from typing import Any

from tools.cq.cli_app.command_schema import (
    BytecodeSurfaceCommandSchema,
    CommonFilterCommandSchema,
    ExceptionsCommandSchema,
    ImpactCommandSchema,
    ImportsCommandSchema,
    QueryCommandSchema,
    ReportCommandSchema,
    RunCommandSchema,
    SearchCommandSchema,
    SideEffectsCommandSchema,
    SigImpactCommandSchema,
)
from tools.cq.core.typed_boundary import convert_strict


def has_filters(filters: CommonFilterCommandSchema) -> bool:
    """Return whether any filtering option is enabled."""
    return bool(
        filters.include
        or filters.exclude
        or filters.impact
        or filters.confidence
        or filters.severity
        or filters.limit is not None
    )


class CommonFilters(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Common filter fields shared across CQ commands."""

    @property
    def has_filters(self) -> bool:
        """Check if any filters are configured."""
        return has_filters(self)


class QueryOptions(QueryCommandSchema, frozen=True, kw_only=True):
    """Options for the q query command."""


class SearchOptions(SearchCommandSchema, frozen=True, kw_only=True):
    """Options for the search command."""


class ReportOptions(ReportCommandSchema, frozen=True, kw_only=True):
    """Options for the report command."""


class ImpactOptions(ImpactCommandSchema, frozen=True, kw_only=True):
    """Options for the impact command."""


class ImportsOptions(ImportsCommandSchema, frozen=True, kw_only=True):
    """Options for the imports command."""


class ExceptionsOptions(ExceptionsCommandSchema, frozen=True, kw_only=True):
    """Options for the exceptions command."""


class SigImpactOptions(SigImpactCommandSchema, frozen=True, kw_only=True):
    """Options for the sig-impact command."""


class SideEffectsOptions(SideEffectsCommandSchema, frozen=True, kw_only=True):
    """Options for the side-effects command."""


class BytecodeSurfaceOptions(BytecodeSurfaceCommandSchema, frozen=True, kw_only=True):
    """Options for the bytecode-surface command."""


class RunOptions(RunCommandSchema, frozen=True, kw_only=True):
    """Options for the run command."""


def options_from_params[T](params: Any, *, type_: type[T]) -> T:
    """Convert a CLI params dataclass into a CQ options struct.

    Returns:
    -------
    T
        Parsed options struct of the requested type.
    """
    return convert_strict(params, type_=type_, from_attributes=True)


__all__ = [
    "BytecodeSurfaceOptions",
    "CommonFilters",
    "ExceptionsOptions",
    "ImpactOptions",
    "ImportsOptions",
    "QueryOptions",
    "ReportOptions",
    "RunOptions",
    "SearchOptions",
    "SideEffectsOptions",
    "SigImpactOptions",
    "has_filters",
    "options_from_params",
]
