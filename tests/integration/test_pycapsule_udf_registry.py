"""Integration tests for DataFusion UDF registry snapshots."""

from __future__ import annotations

import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from obs.diagnostics import DiagnosticsCollector

pytest.importorskip("datafusion")


@pytest.mark.integration
def test_udf_registry_snapshot_includes_capsule_payload() -> None:
    """Record UDF registry payloads while executing a UDF query."""
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
    ctx = profile.session_context()
    df = ctx.sql("SELECT stable_hash64('alpha') AS hash_value")
    table = df.to_arrow_table()
    assert table.num_rows == 1
    artifacts = sink.artifacts_snapshot().get("datafusion_udf_registry_v1", [])
    assert artifacts
    payload = artifacts[-1]
    scalar_names = payload.get("scalar", [])
    assert isinstance(scalar_names, list)
    scalar_strings = [name for name in scalar_names if isinstance(name, str)]
    assert "stable_hash64" in scalar_strings
    assert "pycapsule_udfs" in payload
