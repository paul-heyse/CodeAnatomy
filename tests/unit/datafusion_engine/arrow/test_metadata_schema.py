# ruff: noqa: D100, D103, INP001
from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.metadata_schema import schema_identity


def test_schema_identity_reads_schema_metadata() -> None:
    schema = pa.schema([], metadata={b"schema.identity_hash": b"abc"})
    assert schema_identity(schema)["schema.identity_hash"] == "abc"
