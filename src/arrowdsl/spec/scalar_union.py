"""Shared scalar union definitions for spec tables."""

from __future__ import annotations

import pyarrow as pa

SCALAR_UNION_FIELDS = (
    pa.field("null", pa.null()),
    pa.field("bool", pa.bool_()),
    pa.field("int", pa.int64()),
    pa.field("float", pa.float64()),
    pa.field("string", pa.string()),
    pa.field("binary", pa.binary()),
)

SCALAR_UNION_TYPE = pa.union(list(SCALAR_UNION_FIELDS), mode="dense")


__all__ = ["SCALAR_UNION_FIELDS", "SCALAR_UNION_TYPE"]
