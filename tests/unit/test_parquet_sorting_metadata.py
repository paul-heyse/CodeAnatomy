"""Tests for Parquet sorting metadata emission."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arrowdsl.core.context import OrderingLevel
from arrowdsl.io.parquet import parquet_supports_sorting_columns, write_dataset_parquet
from arrowdsl.schema.metadata import ordering_metadata_spec


def test_parquet_sorting_metadata_written(tmp_path: Path) -> None:
    """Emit sorting columns when explicit ordering metadata is present."""
    if not parquet_supports_sorting_columns():
        pytest.skip("PyArrow sorting_columns not supported in this environment.")
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    schema = ordering_metadata_spec(
        OrderingLevel.EXPLICIT,
        keys=(("id", "ascending"),),
    ).apply(schema)
    table = pa.Table.from_pydict({"id": [1, 2], "value": ["a", "b"]}, schema=schema)
    dataset_dir = tmp_path / "dataset"
    write_dataset_parquet(table, dataset_dir)
    files = list(dataset_dir.glob("*.parquet"))
    assert files
    metadata = pq.ParquetFile(str(files[0])).metadata
    row_group = metadata.row_group(0)
    sorting_columns = getattr(row_group, "sorting_columns", None)
    assert sorting_columns is not None
    assert sorting_columns[0].column_index == 0
    assert sorting_columns[0].descending is False
