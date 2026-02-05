"""Shared search models for smart search."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.search.classifier import QueryMode
from tools.cq.search.profiles import SearchLimits


class SearchKwargs(TypedDict, total=False):
    """Keyword options for smart search entrypoints."""

    mode: QueryMode | None
    include_globs: list[str] | None
    exclude_globs: list[str] | None
    include_strings: bool
    limits: SearchLimits | None
    tc: Toolchain | None
    argv: list[str] | None
    started_ms: float | None


class SearchConfig(CqStruct, frozen=True):
    """Resolved configuration for smart search execution."""

    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    argv: list[str] = msgspec.field(default_factory=list)
    tc: Toolchain | None = None
    started_ms: float = 0.0


__all__ = [
    "SearchConfig",
    "SearchKwargs",
]
