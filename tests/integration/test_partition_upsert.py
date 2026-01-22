"""Tests for partitioned dataset upserts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from datafusion_engine.runtime import read_delta_as_reader
from storage.deltalake import (
    DeltaUpsertOptions,
    upsert_dataset_partitions_delta,
)
from tests.utils import values_as_list


def _read_partition(base_dir: Path, file_id: str) -> pa.Table:
    table = read_delta_as_reader(str(base_dir)).read_all()
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
    assert values_as_list(part_a["value"]) == [1]
    assert values_as_list(part_b["value"]) == [2]

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
    assert values_as_list(part_a["value"]) == [1]
    assert values_as_list(part_b["value"]) == [3]

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
    assert values_as_list(part_b["value"]) == [3]
