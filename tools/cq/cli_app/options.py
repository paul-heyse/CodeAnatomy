"""Typed option structs for cq CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import msgspec

from tools.cq.cli_app.types import QueryLanguageToken
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import convert_strict
from tools.cq.run.spec import RunStep


class CommonFilters(CqStruct, frozen=True, kw_only=True):
    """Common filter fields shared across CQ commands."""

    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)
    impact: list[str] = msgspec.field(default_factory=list)
    confidence: list[str] = msgspec.field(default_factory=list)
    severity: list[str] = msgspec.field(default_factory=list)
    limit: int | None = None

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

    explain_files: bool = False


class SearchOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the search command."""

    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    with_neighborhood: bool = False
    enrich: bool = True
    enrich_mode: str = "ts_sym"
    in_dir: str | None = None
    lang: QueryLanguageToken = QueryLanguageToken.auto


class ReportOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the report command."""

    target: str
    in_dir: str | None = None
    param: str | None = None
    signature: str | None = None
    bytecode_show: str | None = None


class ImpactOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the impact command."""

    param: str
    depth: int = 5


class ImportsOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the imports command."""

    cycles: bool = False
    module: str | None = None


class ExceptionsOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the exceptions command."""

    function: str | None = None


class SigImpactOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the sig-impact command."""

    to: str


class SideEffectsOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the side-effects command."""

    max_files: int = 2000


class BytecodeSurfaceOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the bytecode-surface command."""

    show: str = "globals,attrs,constants"


class RunOptions(CommonFilters, frozen=True, kw_only=True):
    """Options for the run command."""

    plan: Path | None = None
    step: list[RunStep] = msgspec.field(default_factory=list)
    steps: list[RunStep] = msgspec.field(default_factory=list)
    stop_on_error: bool = False


def options_from_params[T](params: Any, *, type_: type[T]) -> T:
    """Convert a CLI params dataclass into a CQ options struct.

    Returns:
    -------
    T
        Parsed options struct of the requested type.
    """
    return convert_strict(params, type_=type_, from_attributes=True)
