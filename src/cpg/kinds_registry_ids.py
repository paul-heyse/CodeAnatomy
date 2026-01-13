"""Shared helpers for derivation ID recipes and join keys."""

from __future__ import annotations

from collections.abc import Sequence

SPAN_CONTAINED_BEST: tuple[str, str] = (
    "interval_align: left.path == right.path",
    "interval_align: right.bstart>=left.bstart AND right.bend<=left.bend (CONTAINED_BEST)",
)


def sha_id(prefix: str, *parts: str) -> str:
    """Return a standardized sha ID recipe string.

    Returns
    -------
    str
        Human-readable sha ID recipe.

    Raises
    ------
    ValueError
        If no parts are provided for the ID recipe.
    """
    if not parts:
        msg = "sha_id requires at least one part."
        raise ValueError(msg)
    joined = "+':'+".join(parts)
    return f"edge_id = sha('{prefix}:'+{joined})[:16]"


def stable_span_id(output_name: str, kind: str) -> str:
    """Return a stable_id recipe string for source spans.

    Returns
    -------
    str
        Human-readable stable_id recipe.
    """
    return f"{output_name} = stable_id(path, bstart, bend, '{kind}')"


def prefixed_span_id(output_name: str, prefix: str) -> str:
    """Return a stable_id recipe string with a custom prefix.

    Returns
    -------
    str
        Human-readable stable_id recipe.
    """
    return f"{output_name} = stable_id(path, bstart, bend, '{prefix}')"


def span_hash_id(prefix: str, *parts: str) -> str:
    """Return a span-based sha ID recipe string.

    Returns
    -------
    str
        Human-readable sha ID recipe.

    Raises
    ------
    ValueError
        If no parts are provided for the ID recipe.
    """
    if not parts:
        msg = "span_hash_id requires at least one part."
        raise ValueError(msg)
    joined = "+':'+".join(parts)
    return f"edge_id = sha('{prefix}:'+{joined})[:16]"


def join_key_list(keys: Sequence[str]) -> tuple[str, ...]:
    """Return a tuple of join key strings.

    Returns
    -------
    tuple[str, ...]
        Join key tuple.
    """
    return tuple(keys)


__all__ = [
    "SPAN_CONTAINED_BEST",
    "join_key_list",
    "prefixed_span_id",
    "sha_id",
    "span_hash_id",
    "stable_span_id",
]
