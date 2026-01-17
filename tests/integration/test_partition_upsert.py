"""Tests for partitioned dataset upserts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.io.parquet import upsert_dataset_partitions_parquet


def _read_partition(base_dir: Path, file_id: str) -> pa.Table:
    dataset = ds.dataset(str(base_dir), format="parquet", partitioning="hive")
    return dataset.to_table(filter=ds.field("file_id") == file_id)


def test_upsert_dataset_partitions_parquet(tmp_path: Path) -> None:
    """Verify that partition upserts replace only targeted partitions."""
    base_dir = tmp_path / "dataset"
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})
    upsert_dataset_partitions_parquet(
        table,
        base_dir=base_dir,
        partition_cols=("file_id",),
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a["value"].to_pylist() == [1]
    assert part_b["value"].to_pylist() == [2]

    update = pa.table({"file_id": ["b"], "value": [3]})
    upsert_dataset_partitions_parquet(
        update,
        base_dir=base_dir,
        partition_cols=("file_id",),
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a["value"].to_pylist() == [1]
    assert part_b["value"].to_pylist() == [3]

    empty = table.slice(0, 0)
    upsert_dataset_partitions_parquet(
        empty,
        base_dir=base_dir,
        partition_cols=("file_id",),
        delete_partitions=({"file_id": "a"},),
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a.num_rows == 0
    assert part_b["value"].to_pylist() == [3]
