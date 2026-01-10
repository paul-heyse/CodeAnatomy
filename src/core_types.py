from __future__ import annotations

"""Shared type aliases and small typing helpers."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal, NewType, TypeAlias

PathLike: TypeAlias = str | Path
StrictnessMode: TypeAlias = Literal["strict", "tolerant"]

RepoId = NewType("RepoId", int)

JsonPrimitive: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonPrimitive | Mapping[str, "JsonValue"] | Sequence["JsonValue"]
JsonDict: TypeAlias = dict[str, JsonValue]


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
