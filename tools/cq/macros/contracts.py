"""Shared contracts for CQ macro execution surfaces."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import msgspec

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

BucketLevel = Literal["low", "med", "high"]


class MacroRequestBase(CqStruct, frozen=True):
    """Base request envelope shared by macro entry points."""

    tc: Toolchain
    root: Path
    argv: list[str]


class ScopedMacroRequestBase(MacroRequestBase, frozen=True):
    """Macro request base with shared include/exclude scope filters."""

    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)


class CallsRequest(MacroRequestBase, frozen=True):
    """Request struct for calls macro execution."""

    function_name: str


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
    impact_bucket: BucketLevel = "low"
    confidence_bucket: BucketLevel = "low"
    details: dict[str, object] = msgspec.field(default_factory=dict)


class ScoringDetailsV1(CqStruct, frozen=True):
    """Normalized scoring details for macro findings."""

    impact_score: float
    impact_bucket: BucketLevel
    confidence_score: float
    confidence_bucket: BucketLevel
    evidence_kind: str


__all__ = [
    "BucketLevel",
    "CallsRequest",
    "MacroExecutionRequestV1",
    "MacroRequestBase",
    "MacroScorePayloadV1",
    "MacroTargetResolutionV1",
    "ScopedMacroRequestBase",
    "ScoringDetailsV1",
]
