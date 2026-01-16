"""Schema definitions for diagnostics datasets."""

from __future__ import annotations

import pyarrow as pa

DATAFUSION_FALLBACKS_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("error", pa.string(), nullable=False),
        pa.field("expression_type", pa.string(), nullable=False),
        pa.field("sql", pa.string(), nullable=False),
        pa.field("dialect", pa.string(), nullable=False),
        pa.field("policy_violations", pa.list_(pa.string()), nullable=True),
    ],
    metadata={b"schema_name": b"datafusion_fallbacks_v1"},
)

DATAFUSION_EXPLAINS_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64(), nullable=False),
        pa.field("sql", pa.string(), nullable=False),
        pa.field("explain_rows_json", pa.string(), nullable=False),
        pa.field("explain_analyze", pa.bool_(), nullable=False),
    ],
    metadata={b"schema_name": b"datafusion_explains_v1"},
)

__all__ = ["DATAFUSION_EXPLAINS_V1", "DATAFUSION_FALLBACKS_V1"]
