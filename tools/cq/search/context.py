"""Context models for smart search execution."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct
from tools.cq.search.profiles import SearchLimits

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.search.classifier import QueryMode


class SmartSearchContext(CqStruct, frozen=True):
    """Resolved context for Smart Search execution."""

    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    argv: list[str]
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    tc: Toolchain | None = None
    started_ms: float = 0.0
