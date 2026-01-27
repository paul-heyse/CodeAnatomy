"""Stable identifier helpers shared by extractors."""

from __future__ import annotations

from datafusion_engine.hash_utils import hash128_from_text

_NULL_SEPARATOR = "\x1f"


def stable_id(prefix: str, *parts: str | None) -> str:
    """Build a deterministic string ID.

    Returns
    -------
    str
        Stable identifier with the requested prefix.
    """
    values: list[str | None] = [prefix, *parts]
    joined = _NULL_SEPARATOR.join(value if value is not None else "None" for value in values)
    hashed = hash128_from_text(joined)
    return f"{prefix}:{hashed}"


def span_id(path: str, bstart: int, bend: int, kind: str | None = None) -> str:
    """Build a stable ID for a source span.

    Returns
    -------
    str
        Stable span identifier.
    """
    if kind:
        return stable_id("span", kind, path, str(bstart), str(bend))
    return stable_id("span", path, str(bstart), str(bend))


__all__ = ["span_id", "stable_id"]
