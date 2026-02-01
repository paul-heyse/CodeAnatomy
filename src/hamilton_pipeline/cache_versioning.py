"""Cache fingerprinting extensions for Arrow/DataFusion outputs."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from hamilton.caching.fingerprinting import hash_value

_REGISTRATION_STATE = {"registered": False}


def register_cache_fingerprinters() -> None:
    """Register Arrow fingerprinting handlers when not already installed."""
    if _REGISTRATION_STATE["registered"]:
        return
    _REGISTRATION_STATE["registered"] = True


@hash_value.register(pa.Table)
def _hash_arrow_table(table: pa.Table, *, depth: int = 0, **kwargs: Any) -> str:
    _ = kwargs
    return hash_value(table.to_pylist(), depth=depth + 1)


@hash_value.register(pa.RecordBatch)
def _hash_arrow_record_batch(batch: pa.RecordBatch, *, depth: int = 0, **kwargs: Any) -> str:
    _ = kwargs
    table = pa.Table.from_batches([batch])
    return hash_value(table.to_pylist(), depth=depth + 1)


@hash_value.register(pa.RecordBatchReader)
def _hash_arrow_record_batch_reader(
    reader: pa.RecordBatchReader,
    *,
    depth: int = 0,
    **kwargs: Any,
) -> str:
    _ = kwargs
    table = reader.read_all()
    return hash_value(table.to_pylist(), depth=depth + 1)


__all__ = ["register_cache_fingerprinters"]
