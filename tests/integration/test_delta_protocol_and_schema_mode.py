"""Integration tests for Delta protocol gating and schema evolution."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("datafusion")
pytest.importorskip("deltalake")

try:  # pragma: no cover - skip when native extensions are unavailable
    import datafusion_ext
except ImportError:  # pragma: no cover - environment without extensions
    pytest.skip("datafusion_ext is unavailable", allow_module_level=True)
if getattr(datafusion_ext, "IS_STUB", False):  # pragma: no cover - stubbed extensions
    pytest.skip("datafusion_ext stub detected", allow_module_level=True)
if not callable(getattr(datafusion_ext, "delta_write_ipc", None)):
    pytest.skip("datafusion_ext.delta_write_ipc unavailable", allow_module_level=True)

from deltalake import DeltaTable

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.delta_protocol import DeltaProtocolSupport
from datafusion_engine.ingest import datafusion_from_arrow
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.write_pipeline import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)
from obs.diagnostics import DiagnosticsCollector
from storage.deltalake import DeltaWriteOptions, write_delta_table
from storage.deltalake.config import DeltaSchemaPolicy


@pytest.mark.integration
def test_schema_mode_merge_allows_new_columns(tmp_path: Path) -> None:
    """Apply schema_mode merge from DeltaSchemaPolicy during writes."""
    delta_path = tmp_path / "delta_table"
    dataset_locations = {
        "events": DatasetLocation(
            path=str(delta_path),
            format="delta",
            delta_schema_policy=DeltaSchemaPolicy(schema_mode="merge"),
        )
    }
    profile = DataFusionRuntimeProfile(
        extract_dataset_locations=dataset_locations,
        enable_schema_registry=False,
        enable_schema_evolution_adapter=False,
    )
    ctx = profile.session_context()
    write_delta_table(
        pa.table({"id": [1], "value": ["a"]}),
        str(delta_path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        ctx=ctx,
    )
    df = datafusion_from_arrow(
        ctx,
        name="events_updates",
        value=pa.table({"id": [2], "value": ["b"], "extra": [1]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
        )
    )
    schema = pa.schema(DeltaTable(str(delta_path)).schema())
    assert "extra" in schema.names


@pytest.mark.integration
def test_delta_protocol_support_warns_and_records(tmp_path: Path) -> None:
    """Record protocol compatibility artifacts when runtime support is insufficient."""
    delta_path = tmp_path / "delta_table"
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(
        diagnostics_sink=sink,
        delta_protocol_support=DeltaProtocolSupport(
            max_reader_version=0,
            max_writer_version=0,
        ),
        delta_protocol_mode="warn",
        enable_schema_registry=False,
        enable_schema_evolution_adapter=False,
    )
    ctx = profile.session_context()
    write_delta_table(
        pa.table({"id": [1], "value": ["a"]}),
        str(delta_path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
        ctx=ctx,
    )
    df = datafusion_from_arrow(
        ctx,
        name="events_more",
        value=pa.table({"id": [2], "value": ["b"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
        )
    )
    artifacts = sink.artifacts_snapshot().get("delta_protocol_compatibility_v1", [])
    assert artifacts
    payload = artifacts[-1]
    assert payload.get("compatible") is False
