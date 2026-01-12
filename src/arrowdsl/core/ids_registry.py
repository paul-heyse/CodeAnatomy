"""HashSpec factory helpers for registry modules."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.ids import HashSpec


def hash_spec(
    *,
    prefix: str,
    cols: Sequence[str],
    out_col: str | None = None,
    null_sentinel: str | None = None,
    extra_literals: Sequence[str] = (),
) -> HashSpec:
    """Return a HashSpec with shared defaults applied.

    Returns
    -------
    HashSpec
        Hash spec configured for string IDs with optional overrides.
    """
    if null_sentinel is None:
        return HashSpec(
            prefix=prefix,
            cols=tuple(cols),
            extra_literals=tuple(extra_literals),
            as_string=True,
            out_col=out_col,
        )
    return HashSpec(
        prefix=prefix,
        cols=tuple(cols),
        extra_literals=tuple(extra_literals),
        as_string=True,
        out_col=out_col,
        null_sentinel=null_sentinel,
    )


__all__ = ["hash_spec"]
