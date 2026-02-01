"""Integration tests for semantic incremental overwrite writes."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.session.runtime import read_delta_as_reader
from semantics.incremental.delta_context import DeltaAccessContext
from semantics.incremental.delta_updates import OverwriteDatasetSpec, write_overwrite_dataset
from semantics.incremental.runtime import IncrementalRuntime
from semantics.incremental.state_store import StateStore


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


def test_write_overwrite_dataset_roundtrip(tmp_path: Path) -> None:
    """Overwrite writes produce a readable Delta table."""
    runtime = _runtime_or_skip()
    context = DeltaAccessContext(runtime)
    schema = pa.schema([("file_id", pa.string()), ("value", pa.int64())])
    table = pa.table({"file_id": ["a"], "value": [1]}, schema=schema)
    state_store = StateStore(tmp_path)

    result = write_overwrite_dataset(
        table,
        spec=OverwriteDatasetSpec(name="test_dataset", schema=schema),
        state_store=state_store,
        context=context,
    )

    path = Path(result["test_dataset"])
    read_back = read_delta_as_reader(str(path)).read_all()
    assert set(read_back.column_names) == {"file_id", "value"}
    assert read_back.num_rows == 1
