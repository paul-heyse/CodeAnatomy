"""Tests for filename partitioning support."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from storage.dataset_sources import DatasetSourceOptions, normalize_dataset_source
from tests.utils import values_as_list


def test_filename_partitioning_requires_schema(tmp_path: Path) -> None:
    """Require a schema when filename partitioning is selected."""
    with pytest.raises(ValueError, match="Filename partitioning requires a schema"):
        normalize_dataset_source(
            tmp_path,
            options=DatasetSourceOptions(dataset_format="parquet", partitioning="filename"),
        )


def test_filename_partitioning_uses_schema(tmp_path: Path) -> None:
    """Open datasets when filename partitioning is configured."""
    schema = pa.schema([("run_id", pa.int64())])
    table = pa.table({"run_id": [1]})
    path = tmp_path / "run_id=1.parquet"
    pq.write_table(table, str(path))
    dataset = normalize_dataset_source(
        tmp_path,
        options=DatasetSourceOptions(
            dataset_format="parquet",
            partitioning="filename",
            filename_partitioning_schema=schema,
        ),
    )
    assert isinstance(dataset, ds.Dataset)
    partitioning = getattr(dataset, "partitioning", None)
    if partitioning is not None:
        assert isinstance(partitioning, ds.FilenamePartitioning)


def test_filename_partitioning_extracts_keys(tmp_path: Path) -> None:
    """Extract partition values from filename templates."""
    schema = pa.schema([("run_id", pa.int64())])
    table = pa.table({"value": ["alpha"]})
    path = tmp_path / "run_id=42.parquet"
    pq.write_table(table, str(path))
    dataset = normalize_dataset_source(
        tmp_path,
        options=DatasetSourceOptions(
            dataset_format="parquet",
            partitioning="filename",
            filename_partitioning_schema=schema,
        ),
    )
    output = dataset.scanner().to_table()
    assert values_as_list(output.column("run_id")) == [42]
