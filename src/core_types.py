"""Shared type aliases and small typing helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal, NewType

type PathLike = str | Path
type StrictnessMode = Literal["strict", "tolerant"]

RepoId = NewType("RepoId", int)

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | Mapping[str, JsonValue] | Sequence[JsonValue]
type JsonDict = dict[str, JsonValue]


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
