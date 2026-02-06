"""Delta/DataFusion conformance smoke tests."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from storage.deltalake.delta import query_delta_sql
from tests.harness.plan_bundle import build_plan_manifest_for_sql, persist_plan_artifacts
from tests.harness.profiles import conformance_profile_with_sink
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


@pytest.mark.integration
def test_df_delta_smoke_round_trip(tmp_path: Path) -> None:
    """Write a Delta table and query it through runtime-session provider registration."""
    delta_path = tmp_path / "events_delta"
    profile, sink = conformance_profile_with_sink()
    ctx = profile.session_context()
    source = register_arrow_table(
        ctx,
        name="events_source",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    write_result = pipeline.write(
        WriteRequest(
            source=source,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    assert write_result.delta_result is not None
    assert write_result.delta_result.version is not None
    runtime_caps = sink.artifacts_snapshot().get("datafusion_runtime_capabilities_v1", [])
    assert runtime_caps
    assert runtime_caps[-1].get("strict_native_provider_enabled") is True
    plan_manifest, plan_details = build_plan_manifest_for_sql(
        ctx=ctx,
        session_runtime=profile.session_runtime(),
        sql="SELECT id, label FROM events_source ORDER BY id",
    )
    manifest_path, details_path = persist_plan_artifacts(
        output_dir=tmp_path / "conformance_artifacts",
        plan_manifest=plan_manifest,
        plan_details=plan_details,
    )
    assert manifest_path.exists()
    assert details_path.exists()
    reader = query_delta_sql(
        "SELECT id, label FROM events ORDER BY id",
        {"events": str(delta_path)},
        runtime_profile=profile,
    )
    rows = reader.read_all().to_pylist()
    assert rows == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]
    provider_artifacts = sink.artifacts_snapshot().get("dataset_provider_mode_v1", [])
    assert provider_artifacts
    last = provider_artifacts[-1]
    assert last.get("provider_mode") == "delta_table_provider"
    assert last.get("strict_native_provider_enabled") is True
    assert last.get("strict_native_provider_violation") is False
    service_provider_artifacts = sink.artifacts_snapshot().get("delta_service_provider_v1", [])
    assert service_provider_artifacts
    service_last = service_provider_artifacts[-1]
    assert service_last.get("provider_kind") == "delta"
    assert service_last.get("strict_native_provider_enabled") is True
