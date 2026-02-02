"""Integration tests for incremental partitioned dataset updates."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import read_delta_as_reader
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.delta_updates import PartitionedDatasetSpec, upsert_partitioned_dataset
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.types import IncrementalFileChanges
from tests.test_helpers.optional_deps import require_delta_extension, require_deltalake

require_deltalake()
require_delta_extension()


def _read_delta(path: Path) -> pa.Table:
    return read_delta_as_reader(str(path)).read_all()


def test_upsert_partitioned_dataset_requires_partition_column(tmp_path: Path) -> None:
    """Raise when the partition column is missing from the input table."""
    runtime = _runtime_or_skip()
    context = DeltaAccessContext(runtime)
    spec = PartitionedDatasetSpec(
        name="test_dataset",
        partition_column="file_id",
        schema=pa.schema([("file_id", pa.string()), ("value", pa.int64())]),
    )
    table = pa.table({"value": [1]})
    with pytest.raises(ValueError, match="file_id"):
        upsert_partitioned_dataset(
            table,
            spec=spec,
            base_dir=str(tmp_path / "dataset"),
            changes=IncrementalFileChanges(),
            context=context,
        )


def test_upsert_partitioned_dataset_alignment_and_deletes(tmp_path: Path) -> None:
    """Align schemas and delete partitions during partitioned upserts."""
    runtime = _runtime_or_skip()
    context = DeltaAccessContext(runtime)
    schema = pa.schema(
        [
            ("file_id", pa.string()),
            ("value", pa.int64()),
            ("extra", pa.string()),
        ]
    )
    spec = PartitionedDatasetSpec(
        name="test_dataset",
        partition_column="file_id",
        schema=schema,
    )
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2], "drop_me": ["x", "y"]})
    changes = IncrementalFileChanges(
        changed_file_ids=("a", "b"),
        deleted_file_ids=(),
        full_refresh=False,
    )
    path = upsert_partitioned_dataset(
        table,
        spec=spec,
        base_dir=str(tmp_path / "dataset"),
        changes=changes,
        context=context,
    )
    assert path is not None

    result = _read_delta(Path(path))
    assert "drop_me" not in result.column_names
    assert "extra" in result.column_names
    assert result["extra"].null_count == result.num_rows

    update = pa.table({"file_id": ["b"], "value": [3], "drop_me": ["z"]})
    delete_changes = IncrementalFileChanges(
        changed_file_ids=("b",),
        deleted_file_ids=("a",),
        full_refresh=False,
    )
    _ = upsert_partitioned_dataset(
        update,
        spec=spec,
        base_dir=str(tmp_path / "dataset"),
        changes=delete_changes,
        context=context,
    )
    updated = _read_delta(Path(path))
    file_ids = {value for value in updated["file_id"].to_pylist() if isinstance(value, str)}
    assert file_ids == {"b"}


def _runtime_or_skip() -> IncrementalRuntime:
    try:
        runtime = IncrementalRuntime.build()
        _ = runtime.session_context()
    except ImportError as exc:
        pytest.skip(str(exc))
    else:
        return runtime
    msg = "Incremental runtime unavailable."
    raise RuntimeError(msg)
