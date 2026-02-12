"""Integration tests for Delta protocol gating and schema evolution."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.diagnostics import diagnostic_profile
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()

import datafusion_ext

if getattr(datafusion_ext, "IS_STUB", False):
    msg = "datafusion_ext stub detected; native extension is required."
    raise RuntimeError(msg)
if not callable(getattr(datafusion_ext, "delta_write_ipc", None)):
    msg = "datafusion_ext.delta_write_ipc unavailable; rebuild Rust artifacts."
    raise TypeError(msg)

from deltalake import DeltaTable

from datafusion_engine.dataset.registry import DatasetLocation, DatasetLocationOverrides
from datafusion_engine.delta.protocol import DeltaProtocolSupport
from datafusion_engine.io.write import (
    WriteFormat,
    WriteMode,
    WritePipeline,
    WriteRequest,
)
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    DiagnosticsConfig,
    ExtractOutputConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from schema_spec.contracts import DeltaPolicyBundle
from storage.deltalake.config import DeltaSchemaPolicy


@pytest.mark.integration
def test_schema_mode_merge_allows_new_columns(tmp_path: Path) -> None:
    """Apply schema_mode merge from DeltaSchemaPolicy during writes."""
    delta_path = tmp_path / "delta_table"
    dataset_locations = {
        "events": DatasetLocation(
            path=str(delta_path),
            format="delta",
            overrides=DatasetLocationOverrides(
                delta=DeltaPolicyBundle(schema_policy=DeltaSchemaPolicy(schema_mode="merge"))
            ),
        )
    }
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            extract_output=ExtractOutputConfig(dataset_locations=dataset_locations)
        ),
        features=FeatureGatesConfig(
            enable_schema_registry=False,
            enable_schema_evolution_adapter=False,
        ),
    )
    ctx = profile.session_context()
    seed = register_arrow_table(
        ctx,
        name="events_seed",
        value=pa.table({"id": [1], "value": ["a"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=seed,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={"schema_mode": "overwrite"},
        )
    )
    df = register_arrow_table(
        ctx,
        name="events_updates",
        value=pa.table({"id": [2], "value": ["b"], "extra": [1]}),
    )
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
    profile, sink = diagnostic_profile(
        profile_factory=lambda diagnostics: DataFusionRuntimeProfile(
            diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
            policies=PolicyBundleConfig(
                delta_protocol_support=DeltaProtocolSupport(
                    max_reader_version=0,
                    max_writer_version=0,
                ),
                delta_protocol_mode="warn",
            ),
            features=FeatureGatesConfig(
                enable_schema_registry=False,
                enable_schema_evolution_adapter=False,
            ),
        )
    )
    ctx = profile.session_context()
    seed = register_arrow_table(
        ctx,
        name="events_seed",
        value=pa.table({"id": [1], "value": ["a"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=seed,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={"schema_mode": "overwrite"},
        )
    )
    df = register_arrow_table(
        ctx,
        name="events_more",
        value=pa.table({"id": [2], "value": ["b"]}),
    )
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
