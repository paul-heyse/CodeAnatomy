"""Shared contracts for CQ macro execution surfaces."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain


class MacroRequestBase(CqStruct, frozen=True):
    """Base request envelope shared by macro entry points."""

    tc: Toolchain
    root: Path
    argv: list[str]


class ScopedMacroRequestBase(MacroRequestBase, frozen=True):
    """Macro request base with shared include/exclude scope filters."""

    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)


class MacroExecutionRequestV1(CqStruct, frozen=True):
    """Compatibility envelope for macro execution request payloads."""

    root: Path
    argv: tuple[str, ...] = ()
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


class MacroTargetResolutionV1(CqStruct, frozen=True):
    """Resolved macro target set for file/symbol entry points."""

    target: str
    files: tuple[str, ...] = ()
    target_kind: str = "symbol"


class MacroScorePayloadV1(CqStruct, frozen=True):
    """Lightweight scoring payload for macro summary surfaces."""

    impact: float = 0.0
    confidence: float = 0.0
    impact_bucket: str = "low"
    confidence_bucket: str = "low"
    details: dict[str, object] = msgspec.field(default_factory=dict)


__all__ = [
    "MacroExecutionRequestV1",
    "MacroRequestBase",
    "MacroScorePayloadV1",
    "MacroTargetResolutionV1",
    "ScopedMacroRequestBase",
]
