"""Conformance checks between DataFusion and fallback UDFs."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pyarrow as pa
import pytest
from datafusion import SessionContext

from datafusion_engine.udf_registry import register_datafusion_udfs
from ibis_engine.builtin_udfs import col_to_byte_python, stable_hash64_python


def _wrapped(fn: Callable[..., object]) -> Callable[..., object]:
    wrapped = getattr(fn, "__wrapped__", None)
    if wrapped is None:
        return fn
    return cast("Callable[..., object]", wrapped)


@pytest.mark.integration
def test_datafusion_udf_conformance() -> None:
    """Match DataFusion UDF outputs to python fallbacks."""
    ctx = SessionContext()
    register_datafusion_udfs(ctx)
    table = pa.table(
        {
            "value": ["alpha", None],
            "line": ["éclair", "abc"],
            "offset": [2, 1],
            "unit": ["utf8", "utf32"],
        }
    )
    ctx.register_record_batches("input_table", [table.to_batches()])
    query = (
        "select stable_hash64(value) as h, col_to_byte(line, offset, unit) as b from input_table"
    )
    df = ctx.sql(query)
    result = df.to_arrow_table().to_pylist()
    hash_fn = _wrapped(stable_hash64_python)
    col_fn = _wrapped(col_to_byte_python)
    expected = [
        {
            "h": hash_fn(value),
            "b": col_fn(line, offset, unit),
        }
        for value, line, offset, unit in zip(
            table.column("value").to_pylist(),
            table.column("line").to_pylist(),
            table.column("offset").to_pylist(),
            table.column("unit").to_pylist(),
            strict=True,
        )
    ]
    assert result == expected
