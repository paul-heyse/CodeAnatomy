"""Shared type aliases and small typing helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, NewType

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


class DeterminismTier(StrEnum):
    """Determinism budgets for the pipeline."""

    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"
    FAST = "best_effort"
    STABLE = "stable_set"


def parse_determinism_tier(value: DeterminismTier | str | None) -> DeterminismTier | None:
    """Parse a determinism tier from string or enum input.

    Returns
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

    Returns
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
    "JsonDict",
    "JsonPrimitive",
    "JsonValue",
    "JsonValueLax",
    "PathLike",
    "RepoId",
    "RowPermissive",
    "RowRich",
    "RowStrict",
    "RowValueRich",
    "RowValueStrict",
    "StrictnessMode",
    "ensure_path",
    "parse_determinism_tier",
]
