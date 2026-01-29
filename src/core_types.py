"""Shared type aliases and small typing helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Literal, NewType

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


class DeterminismTier(StrEnum):
    """Determinism budgets for the pipeline."""

    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"
    FAST = "best_effort"
    STABLE = "stable_set"


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
