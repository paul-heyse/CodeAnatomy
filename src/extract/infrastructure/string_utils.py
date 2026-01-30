"""Shared string normalization helpers for extract pipelines."""

from __future__ import annotations

from collections.abc import Sequence


def normalize_string_items(items: Sequence[object]) -> list[str | None]:
    """Normalize a sequence of values into optional strings.

    Returns
    -------
    list[str | None]
        Normalized string values.
    """
    out: list[str | None] = []
    for item in items:
        if item is None:
            out.append(None)
        elif isinstance(item, str):
            out.append(item)
        else:
            out.append(str(item))
    return out


__all__ = ["normalize_string_items"]
