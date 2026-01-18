"""Conformance checks for UDF fallbacks."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pyarrow as pa

from ibis_engine.builtin_udfs import (
    col_to_byte_pyarrow,
    col_to_byte_python,
    stable_hash64_pyarrow,
    stable_hash64_python,
)


def _wrapped(fn: Callable[..., object]) -> Callable[..., object]:
    wrapped = getattr(fn, "__wrapped__", None)
    if wrapped is None:
        return fn
    return cast("Callable[..., object]", wrapped)


def test_stable_hash64_fallbacks_match() -> None:
    """Ensure pyarrow and python fallbacks agree."""
    values = pa.array(["alpha", None, "beta"])
    pyarrow_fn = _wrapped(stable_hash64_pyarrow)
    python_fn = _wrapped(stable_hash64_python)
    pyarrow_out = cast("pa.Array", pyarrow_fn(values))
    expected = [python_fn(value) for value in values.to_pylist()]
    assert pyarrow_out.to_pylist() == expected


def test_col_to_byte_fallbacks_match() -> None:
    """Ensure col_to_byte fallbacks agree."""
    lines = pa.array(["abc", "éclair", None])
    offsets = pa.array([1, 2, 3])
    units = pa.array(["utf32", "utf8", "utf32"])
    pyarrow_fn = _wrapped(col_to_byte_pyarrow)
    python_fn = _wrapped(col_to_byte_python)
    pyarrow_out = cast("pa.Array", pyarrow_fn(lines, offsets, units))
    expected = [
        python_fn(line, offset, unit)
        for line, offset, unit in zip(
            lines.to_pylist(),
            offsets.to_pylist(),
            units.to_pylist(),
            strict=True,
        )
    ]
    assert pyarrow_out.to_pylist() == expected
