"""Unit tests for incremental streaming diagnostics."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from incremental import delta_updates
from incremental.delta_updates import OverwriteDatasetSpec, write_overwrite_dataset
from incremental.runtime import IncrementalRuntime
from incremental.state_store import StateStore
from obs.diagnostics import DiagnosticsCollector
from sqlglot_tools.optimizer import default_sqlglot_policy

EXPECTED_ROW_COUNT = 2


def test_streaming_write_records_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Record streaming diagnostics when overwrite outputs exceed the threshold."""
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    runtime = IncrementalRuntime(profile=profile, sqlglot_policy=default_sqlglot_policy())
    monkeypatch.setattr(delta_updates, "_STREAMING_ROW_THRESHOLD", 1)

    schema = pa.schema([("file_id", pa.string()), ("value", pa.int64())])
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})
    state_store = StateStore(tmp_path)
    spec = OverwriteDatasetSpec(name="test_dataset", schema=schema)
    _ = write_overwrite_dataset(
        table,
        spec=spec,
        state_store=state_store,
        runtime=runtime,
    )

    artifacts = sink.artifacts_snapshot().get("incremental_streaming_writes_v1", [])
    assert artifacts
    payload = artifacts[0]
    assert payload["dataset_name"] == "test_dataset"
    assert payload["row_count"] == EXPECTED_ROW_COUNT
