"""Specification types for ID/hash column generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type MissingPolicy = Literal["raise", "null"]


@dataclass(frozen=True)
class HashSpec:
    """Specification for a hash-based ID column.

    Parameters
    ----------
    prefix:
        Prefix for the ID string.
    cols:
        Columns included in the hash input.
    extra_literals:
        Literal string parts to include in the hash input.
    as_string:
        When ``True``, return a prefixed string ID; otherwise an int64 hash.
    null_sentinel:
        Sentinel value for nulls in the hash input.
    out_col:
        Output column name override.
    """

    prefix: str
    cols: tuple[str, ...]
    extra_literals: tuple[str, ...] = ()
    as_string: bool = True
    null_sentinel: str = "__NULL__"
    out_col: str | None = None
    missing: MissingPolicy = "raise"
