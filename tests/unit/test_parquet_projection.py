"""Projection schema enforcement for dataset writes."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from arrowdsl.io.parquet import DatasetWriteConfig, write_dataset_parquet


def test_projection_schema_requires_subset(tmp_path: Path) -> None:
    """Reject schemas that are not projection-only subsets."""
    table = pa.table({"entity_id": [1, 2], "name": ["alpha", "beta"]})
    bad_schema = pa.schema(
        [
            pa.field("entity_id", pa.string()),
            pa.field("extra", pa.string()),
        ]
    )
    config = DatasetWriteConfig(schema=bad_schema, overwrite=True)
    with pytest.raises(ValueError, match="projection-only"):
        write_dataset_parquet(table, tmp_path / "bad_schema", config=config)


def test_projection_schema_subset_ok(tmp_path: Path) -> None:
    """Allow projection-only schema subsets."""
    table = pa.table({"entity_id": [1, 2], "name": ["alpha", "beta"]})
    good_schema = pa.schema([pa.field("entity_id", pa.int64())])
    config = DatasetWriteConfig(schema=good_schema, overwrite=True)
    path = write_dataset_parquet(table, tmp_path / "good_schema", config=config)
    dataset = ds.dataset(path, format="parquet")
    assert dataset.schema.names == ["entity_id"]
