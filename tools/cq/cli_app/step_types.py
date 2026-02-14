"""CLI parsing types for cq run steps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class QStepCli:
    """CLI payload for a q step."""

    type: Literal["q"] = "q"
    id: str | None = None
    query: str | None = None


@dataclass(frozen=True)
class SearchStepCli:
    """CLI payload for a search step."""

    type: Literal["search"] = "search"
    id: str | None = None
    query: str | None = None
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None
    lang_scope: str = "auto"


@dataclass(frozen=True)
class CallsStepCli:
    """CLI payload for a calls step."""

    type: Literal["calls"] = "calls"
    id: str | None = None
    function: str | None = None


@dataclass(frozen=True)
class ImpactStepCli:
    """CLI payload for an impact step."""

    type: Literal["impact"] = "impact"
    id: str | None = None
    function: str | None = None
    param: str | None = None
    depth: int = 5


@dataclass(frozen=True)
class ImportsStepCli:
    """CLI payload for an imports step."""

    type: Literal["imports"] = "imports"
    id: str | None = None
    cycles: bool = False
    module: str | None = None


@dataclass(frozen=True)
class ExceptionsStepCli:
    """CLI payload for an exceptions step."""

    type: Literal["exceptions"] = "exceptions"
    id: str | None = None
    function: str | None = None


@dataclass(frozen=True)
class SigImpactStepCli:
    """CLI payload for a sig-impact step."""

    type: Literal["sig-impact"] = "sig-impact"
    id: str | None = None
    symbol: str | None = None
    to: str | None = None


@dataclass(frozen=True)
class SideEffectsStepCli:
    """CLI payload for a side-effects step."""

    type: Literal["side-effects"] = "side-effects"
    id: str | None = None
    max_files: int = 2000


@dataclass(frozen=True)
class ScopesStepCli:
    """CLI payload for a scopes step."""

    type: Literal["scopes"] = "scopes"
    id: str | None = None
    target: str | None = None
    max_files: int = 500


@dataclass(frozen=True)
class BytecodeSurfaceStepCli:
    """CLI payload for a bytecode-surface step."""

    type: Literal["bytecode-surface"] = "bytecode-surface"
    id: str | None = None
    target: str | None = None
    show: str = "globals,attrs,constants"
    max_files: int = 500


@dataclass(frozen=True)
class NeighborhoodStepCli:
    """CLI payload for a neighborhood step."""

    type: Literal["neighborhood"] = "neighborhood"
    id: str | None = None
    target: str | None = None
    lang: str = "python"
    top_k: int = 10
    no_semantic_enrichment: bool = False


RunStepCli = (
    QStepCli
    | SearchStepCli
    | CallsStepCli
    | ImpactStepCli
    | ImportsStepCli
    | ExceptionsStepCli
    | SigImpactStepCli
    | SideEffectsStepCli
    | ScopesStepCli
    | BytecodeSurfaceStepCli
    | NeighborhoodStepCli
)


__all__ = [
    "BytecodeSurfaceStepCli",
    "CallsStepCli",
    "ExceptionsStepCli",
    "ImpactStepCli",
    "ImportsStepCli",
    "NeighborhoodStepCli",
    "QStepCli",
    "RunStepCli",
    "ScopesStepCli",
    "SearchStepCli",
    "SideEffectsStepCli",
    "SigImpactStepCli",
]
