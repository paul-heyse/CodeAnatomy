"""Tests for partitioned dataset upserts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.runtime import read_delta_as_reader
from incremental.delta_context import DeltaAccessContext
from incremental.delta_updates import PartitionedDatasetSpec, upsert_partitioned_dataset
from incremental.runtime import IncrementalRuntime
from incremental.types import IncrementalFileChanges
from tests.utils import values_as_list


def _read_partition(base_dir: Path, file_id: str) -> pa.Table:
    table = read_delta_as_reader(str(base_dir)).read_all()
    mask = table["file_id"] == pa.scalar(file_id)
    return table.filter(mask)


def test_upsert_dataset_partitions_delta(tmp_path: Path) -> None:
    """Verify that partition upserts replace only targeted partitions."""
    try:
        runtime = IncrementalRuntime.build()
        _ = runtime.ibis_backend()
    except ImportError as exc:
        pytest.skip(str(exc))
    base_dir = tmp_path / "dataset"
    context = DeltaAccessContext(runtime)
    spec = PartitionedDatasetSpec(name="test_dataset", partition_column="file_id")
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})
    upsert_partitioned_dataset(
        table,
        spec=spec,
        base_dir=str(base_dir),
        changes=IncrementalFileChanges(),
        context=context,
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert values_as_list(part_a["value"]) == [1]
    assert values_as_list(part_b["value"]) == [2]

    update = pa.table({"file_id": ["b"], "value": [3]})
    upsert_partitioned_dataset(
        update,
        spec=spec,
        base_dir=str(base_dir),
        changes=IncrementalFileChanges(),
        context=context,
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert values_as_list(part_a["value"]) == [1]
    assert values_as_list(part_b["value"]) == [3]

    empty = table.slice(0, 0)
    upsert_partitioned_dataset(
        empty,
        spec=spec,
        base_dir=str(base_dir),
        changes=IncrementalFileChanges(deleted_file_ids=("a",)),
        context=context,
    )

    part_a = _read_partition(base_dir, "a")
    part_b = _read_partition(base_dir, "b")
    assert part_a.num_rows == 0
    assert values_as_list(part_b["value"]) == [3]
