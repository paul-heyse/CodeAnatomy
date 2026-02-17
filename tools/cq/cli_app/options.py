"""Typed option structs for cq CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import msgspec

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
from tools.cq.cli_app.types import QueryLanguageToken
from tools.cq.core.typed_boundary import convert_strict
from tools.cq.run.spec import RunStep


class CommonFilters(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Common filter fields shared across CQ commands."""

    @property
    def has_filters(self) -> bool:
        """Check if any filters are configured.

        Returns:
        -------
        bool
            ``True`` when at least one filter is set.
        """
        return bool(
            self.include
            or self.exclude
            or self.impact
            or self.confidence
            or self.severity
            or self.limit is not None
        )


class QueryOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the q query command."""

    explain_files: bool = QueryCommandSchema.explain_files


class SearchOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the search command."""

    regex: bool = SearchCommandSchema.regex
    literal: bool = SearchCommandSchema.literal
    include_strings: bool = SearchCommandSchema.include_strings
    with_neighborhood: bool = SearchCommandSchema.with_neighborhood
    enrich: bool = SearchCommandSchema.enrich
    enrich_mode: str = SearchCommandSchema.enrich_mode
    in_dir: str | None = SearchCommandSchema.in_dir
    lang: QueryLanguageToken = SearchCommandSchema.lang


class ReportOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the report command."""

    target: str
    in_dir: str | None = ReportCommandSchema.in_dir
    param: str | None = ReportCommandSchema.param
    signature: str | None = ReportCommandSchema.signature
    bytecode_show: str | None = ReportCommandSchema.bytecode_show


class ImpactOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the impact command."""

    param: str
    depth: int = ImpactCommandSchema.depth


class ImportsOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the imports command."""

    cycles: bool = ImportsCommandSchema.cycles
    module: str | None = ImportsCommandSchema.module


class ExceptionsOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the exceptions command."""

    function: str | None = ExceptionsCommandSchema.function


class SigImpactOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the sig-impact command."""

    to: str


class SideEffectsOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the side-effects command."""

    max_files: int = SideEffectsCommandSchema.max_files


class BytecodeSurfaceOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the bytecode-surface command."""

    show: str = BytecodeSurfaceCommandSchema.show


class RunOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the run command."""

    plan: Path | None = None
    step: list[RunStep] = msgspec.field(default_factory=list)
    steps: list[RunStep] = msgspec.field(default_factory=list)
    stop_on_error: bool = RunCommandSchema.stop_on_error


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
    "options_from_params",
]
