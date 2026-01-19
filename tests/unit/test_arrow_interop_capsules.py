"""Tests for Arrow C protocol ingestion helpers."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.core.interop import coerce_table_like, table_from_arrow_c_array
from storage.dataset_sources import DatasetSourceOptions, normalize_dataset_source


def test_table_from_arrow_c_array_imports_pyarrow_array() -> None:
    """Import a pyarrow Array via the C array protocol."""
    array = pa.array([1, 2, 3])
    table = table_from_arrow_c_array(array, name="values")
    assert table.column_names == ["values"]
    assert table.to_pydict()["values"] == [1, 2, 3]


def test_coerce_table_like_handles_arrow_c_array() -> None:
    """Coerce Arrow C array providers into tables."""
    array = pa.array([4, 5])
    table = cast("pa.Table", coerce_table_like(array))
    assert table.to_pydict()["value"] == [4, 5]


def test_coerce_table_like_keeps_record_batch_reader() -> None:
    """Return readers unchanged when already reader-like."""
    batch = pa.record_batch([pa.array([1])], names=["a"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    result = coerce_table_like(reader)
    assert result is reader


def test_normalize_dataset_source_accepts_arrow_c_array() -> None:
    """Normalize datasets from Arrow C array providers."""
    array = pa.array([10, 20])
    options = DatasetSourceOptions(dataset_format="parquet")
    dataset = normalize_dataset_source(array, options=options)
    assert isinstance(dataset, ds.Dataset)
    assert dataset.schema.names == ["value"]
