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
    return filters.has_filters


CommonFilters = CommonFilterCommandSchema
QueryOptions = QueryCommandSchema
SearchOptions = SearchCommandSchema
ReportOptions = ReportCommandSchema
ImpactOptions = ImpactCommandSchema
ImportsOptions = ImportsCommandSchema
ExceptionsOptions = ExceptionsCommandSchema
SigImpactOptions = SigImpactCommandSchema
SideEffectsOptions = SideEffectsCommandSchema
BytecodeSurfaceOptions = BytecodeSurfaceCommandSchema
RunOptions = RunCommandSchema


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
