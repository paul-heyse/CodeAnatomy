from __future__ import annotations

from pathlib import Path
from typing import Dict, Literal, Mapping, NewType, Sequence, Union

PathLike = Union[str, Path]
StrictnessMode = Literal["strict", "tolerant"]

RepoId = NewType("RepoId", int)

JsonPrimitive = Union[str, int, float, bool, None]
JsonValue = Union[JsonPrimitive, Mapping[str, "JsonValue"], Sequence["JsonValue"]]
JsonDict = Dict[str, JsonValue]


def ensure_path(p: PathLike) -> Path:
    """Normalize a string/Path to a Path."""
    return p if isinstance(p, Path) else Path(p)
