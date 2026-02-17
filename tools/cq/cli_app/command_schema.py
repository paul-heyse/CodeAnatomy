"""Canonical command schema definitions for CLI projection layers."""

from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.cli_app.types import NeighborhoodLanguageToken, QueryLanguageToken
from tools.cq.core.structs import CqStruct
from tools.cq.run.spec import RunStep


class CommonFilterCommandSchema(CqStruct, frozen=True, kw_only=True):
    """Canonical filter schema shared by command projections."""

    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)
    impact: list[str] = msgspec.field(default_factory=list)
    confidence: list[str] = msgspec.field(default_factory=list)
    severity: list[str] = msgspec.field(default_factory=list)
    limit: int | None = None

    @property
    def has_filters(self) -> bool:
        """Return whether any filter option is enabled."""
        return bool(
            self.include
            or self.exclude
            or self.impact
            or self.confidence
            or self.severity
            or self.limit is not None
        )


class SearchCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical search command schema."""

    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    with_neighborhood: bool = False
    enrich: bool = True
    enrich_mode: str = "ts_sym"
    in_dir: str | None = None
    lang: QueryLanguageToken = QueryLanguageToken.auto


class NeighborhoodCommandSchema(CqStruct, frozen=True, kw_only=True):
    """Canonical neighborhood command schema."""

    lang: NeighborhoodLanguageToken = NeighborhoodLanguageToken.python
    top_k: int = 10
    semantic_enrichment: bool = True
    enrich: bool = True
    enrich_mode: str = "ts_sym"


class QueryCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical q command schema."""

    explain_files: bool = False


class ReportCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical report command schema."""

    target: str
    in_dir: str | None = None
    param: str | None = None
    signature: str | None = None
    bytecode_show: str | None = None


class ImpactCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical impact command schema."""

    param: str
    depth: int = 5


class ImportsCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical imports command schema."""

    cycles: bool = False
    module: str | None = None


class ExceptionsCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical exceptions command schema."""

    function: str | None = None


class SigImpactCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical sig-impact command schema."""

    to: str


class SideEffectsCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical side-effects command schema."""

    max_files: int = 2000


class BytecodeSurfaceCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical bytecode-surface command schema."""

    show: str = "globals,attrs,constants"


class RunCommandSchema(CommonFilterCommandSchema, frozen=True, kw_only=True):
    """Canonical run command schema."""

    plan: Path | None = None
    step: list[RunStep] = msgspec.field(default_factory=list)
    steps: list[RunStep] = msgspec.field(default_factory=list)
    stop_on_error: bool = False


__all__ = [
    "BytecodeSurfaceCommandSchema",
    "CommonFilterCommandSchema",
    "ExceptionsCommandSchema",
    "ImpactCommandSchema",
    "ImportsCommandSchema",
    "NeighborhoodCommandSchema",
    "QueryCommandSchema",
    "ReportCommandSchema",
    "RunCommandSchema",
    "SearchCommandSchema",
    "SideEffectsCommandSchema",
    "SigImpactCommandSchema",
]
