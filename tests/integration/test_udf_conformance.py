"""Conformance checks for DataFusion UDF outputs."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

import pyarrow as pa
import pytest
from datafusion import SessionContext

from arrowdsl.core.ids import hash64_from_text
from arrowdsl.schema.build import rows_from_table
from datafusion_engine.udf_registry import register_datafusion_udfs
from tests.utils import values_as_list


def _optional_str_list(values: Sequence[object]) -> list[str | None]:
    result: list[str | None] = []
    for value in values:
        if value is None:
            result.append(None)
        elif isinstance(value, str):
            result.append(value)
        else:
            msg = "Expected string or None."
            raise TypeError(msg)
    return result


def _optional_int_list(values: Sequence[object]) -> list[int | None]:
    result: list[int | None] = []
    for value in values:
        if value is None:
            result.append(None)
        elif isinstance(value, bool):
            msg = "Expected int or None."
            raise TypeError(msg)
        elif isinstance(value, int):
            result.append(value)
        else:
            msg = "Expected int or None."
            raise TypeError(msg)
    return result


def _hash128_text(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.blake2b(value.encode("utf-8"), digest_size=16).hexdigest()


def _col_to_byte_reference(line: str | None, offset: int | None, unit: str | None) -> int | None:
    if line is None or offset is None or unit is None:
        return None
    offset_value = max(0, offset)
    if unit == "utf32":
        clamped = min(offset_value, len(line))
        return len(line[:clamped].encode("utf-8"))
    if unit == "utf8":
        encoded = line.encode("utf-8")
        byte_off = min(offset_value, len(encoded))
        decoded = encoded[:byte_off].decode("utf-8", errors="strict")
        return len(decoded.encode("utf-8"))
    msg = f"Unsupported unit for test coverage: {unit}"
    raise ValueError(msg)


@pytest.mark.integration
def test_datafusion_udf_conformance() -> None:
    """Match DataFusion UDF outputs to reference implementations."""
    ctx = SessionContext()
    register_datafusion_udfs(ctx)
    table = pa.table(
        {
            "value": ["alpha", None],
            "line": ["abc", "éclair"],
            "offset": [1, 2],
            "unit": ["utf32", "utf8"],
        }
    )
    ctx.register_record_batches("input_table", [table.to_batches()])
    query = (
        "select stable_hash64(value) as h64, stable_hash128(value) as h128, "
        "col_to_byte(line, offset, unit) as b from input_table"
    )
    df = ctx.sql(query)
    result = rows_from_table(df.to_arrow_table())
    values = _optional_str_list(values_as_list(table.column("value")))
    lines = _optional_str_list(values_as_list(table.column("line")))
    offsets = _optional_int_list(values_as_list(table.column("offset")))
    units = _optional_str_list(values_as_list(table.column("unit")))
    expected = [
        {
            "h64": hash64_from_text(value) if value is not None else None,
            "h128": _hash128_text(value),
            "b": _col_to_byte_reference(line, offset, unit),
        }
        for value, line, offset, unit in zip(values, lines, offsets, units, strict=True)
    ]
    assert result == expected
