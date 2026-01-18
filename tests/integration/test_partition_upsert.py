"""Tests for partitioned dataset upserts."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa

from arrowdsl.io.delta import (
    DeltaUpsertOptions,
    read_table_delta,
    upsert_dataset_partitions_delta,
)


def _read_partition(base_dir: Path, file_id: str) -> pa.Table:
    table = cast("pa.Table", read_table_delta(str(base_dir)))
    mask = table["file_id"] == pa.scalar(file_id)
    return table.filter(mask)


def test_upsert_dataset_partitions_delta(tmp_path: Path) -> None:
    """Verify that partition upserts replace only targeted partitions."""
    base_dir = tmp_path / "dataset"
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})
    upsert_dataset_partitions_delta(
        table,
        options=DeltaUpsertOptions(
            base_dir=str(base_dir),
            partition_cols=("file_id",),
        ),
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a["value"].to_pylist() == [1]
    assert part_b["value"].to_pylist() == [2]

    update = pa.table({"file_id": ["b"], "value": [3]})
    upsert_dataset_partitions_delta(
        update,
        options=DeltaUpsertOptions(
            base_dir=str(base_dir),
            partition_cols=("file_id",),
        ),
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a["value"].to_pylist() == [1]
    assert part_b["value"].to_pylist() == [3]

    empty = table.slice(0, 0)
    upsert_dataset_partitions_delta(
        empty,
        options=DeltaUpsertOptions(
            base_dir=str(base_dir),
            partition_cols=("file_id",),
            delete_partitions=({"file_id": "a"},),
        ),
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a.num_rows == 0
    assert part_b["value"].to_pylist() == [3]
