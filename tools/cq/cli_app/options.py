"""Typed option structs for cq CLI commands."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

import msgspec

from tools.cq.core.structs import CqStruct


class CommonFilters(CqStruct, frozen=True):
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

        Returns
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


class QueryOptions(CommonFilters, frozen=True):
    """Options for the q query command."""

    explain_files: bool = False


class SearchOptions(CommonFilters, frozen=True):
    """Options for the search command."""

    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None


class ReportOptions(CommonFilters, frozen=True):
    """Options for the report command."""

    target: str | None = None
    in_dir: str | None = None
    param: str | None = None
    signature: str | None = None
    bytecode_show: str | None = None


class ImpactOptions(CommonFilters, frozen=True):
    """Options for the impact command."""

    param: str | None = None
    depth: int = 5


class ImportsOptions(CommonFilters, frozen=True):
    """Options for the imports command."""

    cycles: bool = False
    module: str | None = None


class ExceptionsOptions(CommonFilters, frozen=True):
    """Options for the exceptions command."""

    function: str | None = None


class SigImpactOptions(CommonFilters, frozen=True):
    """Options for the sig-impact command."""

    to: str | None = None


class SideEffectsOptions(CommonFilters, frozen=True):
    """Options for the side-effects command."""

    max_files: int = 2000


class BytecodeSurfaceOptions(CommonFilters, frozen=True):
    """Options for the bytecode-surface command."""

    show: str = "globals,attrs,constants"


def options_from_params[T](params: Any, *, type_: type[T]) -> T:
    """Convert a CLI params dataclass into a CQ options struct.

    Returns
    -------
    T
        Parsed options struct of the requested type.
    """
    data = asdict(params)
    return msgspec.convert(data, type=type_, strict=True)
