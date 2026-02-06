"""Shared type aliases and small typing helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal, NewType

from msgspec import Meta
from pydantic import Field

type PathLike = str | Path
type StrictnessMode = Literal["strict", "tolerant"]

RepoId = NewType("RepoId", int)

IDENTIFIER_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
RUN_ID_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_-]{7,63}$"
HASH_PATTERN = "^[A-Fa-f0-9]{32,128}$"
EVENT_KIND_PATTERN = "^[a-z][a-z0-9_]{0,63}$"
STATUS_PATTERN = "^[a-z][a-z0-9_-]{0,31}$"

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | Mapping[str, JsonValue] | Sequence[JsonValue]
type JsonDict = dict[str, JsonValue]
type JsonValueLax = Any

type RowValueStrict = str | int
type RowValueRich = str | int | bool | list[str] | list[dict[str, object]] | None
type RowStrict = dict[str, RowValueStrict]
type RowRich = dict[str, RowValueRich]
type RowPermissive = dict[str, object]

PositiveInt = Annotated[int, Meta(gt=0)]
NonNegativeInt = Annotated[int, Meta(ge=0)]
PositiveFloat = Annotated[float, Meta(gt=0)]
NonNegativeFloat = Annotated[float, Meta(ge=0)]

IdentifierStr = Annotated[
    str,
    Meta(
        pattern=IDENTIFIER_PATTERN,
        title="Identifier",
        description="Normalized identifier string.",
    ),
    Field(pattern=IDENTIFIER_PATTERN),
]
RunIdStr = Annotated[
    str,
    Meta(
        pattern=RUN_ID_PATTERN,
        title="Run ID",
        description="Stable run identifier.",
    ),
    Field(pattern=RUN_ID_PATTERN),
]
HashStr = Annotated[
    str,
    Meta(
        pattern=HASH_PATTERN,
        title="Hash Value",
        description="Deterministic hash value.",
        examples=["sha256:4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c"],
    ),
    Field(pattern=HASH_PATTERN),
]
EventKindStr = Annotated[
    str,
    Meta(
        pattern=EVENT_KIND_PATTERN,
        title="Event Kind",
        description="Normalized event kind identifier.",
    ),
    Field(pattern=EVENT_KIND_PATTERN),
]
StatusStr = Annotated[
    str,
    Meta(
        pattern=STATUS_PATTERN,
        title="Status",
        description="Normalized status identifier.",
    ),
    Field(pattern=STATUS_PATTERN),
]


class DeterminismTier(StrEnum):
    """Determinism budgets for the pipeline."""

    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"
    FAST = "best_effort"
    STABLE = "stable_set"


def parse_determinism_tier(value: DeterminismTier | str | None) -> DeterminismTier | None:
    """Parse a determinism tier from string or enum input.

    Returns:
    -------
    DeterminismTier | None
        Parsed determinism tier when available.
    """
    if isinstance(value, DeterminismTier):
        return value
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    mapping: dict[str, DeterminismTier] = {
        "tier2": DeterminismTier.CANONICAL,
        "canonical": DeterminismTier.CANONICAL,
        "tier1": DeterminismTier.STABLE_SET,
        "stable": DeterminismTier.STABLE_SET,
        "stable_set": DeterminismTier.STABLE_SET,
        "tier0": DeterminismTier.BEST_EFFORT,
        "fast": DeterminismTier.BEST_EFFORT,
        "best_effort": DeterminismTier.BEST_EFFORT,
    }
    return mapping.get(normalized)


def ensure_path(p: PathLike) -> Path:
    """Return a normalized ``Path`` for the provided value.

    Parameters
    ----------
    p:
        String or ``Path`` input to normalize.

    Returns:
    -------
    pathlib.Path
        Normalized path instance.
    """
    return p if isinstance(p, Path) else Path(p)


__all__ = [
    "EVENT_KIND_PATTERN",
    "HASH_PATTERN",
    "IDENTIFIER_PATTERN",
    "RUN_ID_PATTERN",
    "STATUS_PATTERN",
    "DeterminismTier",
    "EventKindStr",
    "HashStr",
    "IdentifierStr",
    "JsonDict",
    "JsonPrimitive",
    "JsonValue",
    "JsonValueLax",
    "NonNegativeFloat",
    "NonNegativeInt",
    "PathLike",
    "PositiveFloat",
    "PositiveInt",
    "RepoId",
    "RowPermissive",
    "RowRich",
    "RowStrict",
    "RowValueRich",
    "RowValueStrict",
    "RunIdStr",
    "StatusStr",
    "StrictnessMode",
    "ensure_path",
    "parse_determinism_tier",
]
