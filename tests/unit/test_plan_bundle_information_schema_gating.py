"""Tests for information_schema snapshot gating in plan bundles."""

from __future__ import annotations

from datafusion_engine.plan import bundle as plan_bundle
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, FeatureGatesConfig


def test_information_schema_snapshot_skips_udf_catalog_when_udfs_disabled(
    monkeypatch,
) -> None:
    """Ensure plan snapshots do not query routine metadata with UDFs disabled."""
    profile = DataFusionRuntimeProfile(features=FeatureGatesConfig(enable_udfs=False))
    runtime = profile.session_runtime()
    ctx = runtime.ctx

    class _FakeIntrospector:
        def __init__(self, _ctx, *, sql_options=None) -> None:
            _ = (_ctx, sql_options)

        def tables_snapshot(self) -> list[dict[str, object]]:
            return [{"table_name": "events"}]

        def table_definition(self, _name: str) -> str:
            return "CREATE TABLE events (id BIGINT)"

        def settings_snapshot(self) -> list[dict[str, object]]:
            return []

        def schemata_snapshot(self) -> list[dict[str, object]]:
            return []

        def columns_snapshot(self) -> list[dict[str, object]]:
            return []

        def routines_snapshot(self) -> list[dict[str, object]]:
            msg = "routines_snapshot should be gated off when enable_udfs=False"
            raise AssertionError(msg)

        def parameters_snapshot(self) -> list[dict[str, object]]:
            msg = "parameters_snapshot should be gated off when enable_udfs=False"
            raise AssertionError(msg)

        def function_catalog_snapshot(
            self,
            *,
            include_parameters: bool = False,
        ) -> list[dict[str, object]]:
            _ = include_parameters
            msg = "function_catalog_snapshot should be gated off when enable_udfs=False"
            raise AssertionError(msg)

    monkeypatch.setattr(plan_bundle, "SchemaIntrospector", _FakeIntrospector)

    snapshot = plan_bundle._information_schema_snapshot(ctx, session_runtime=runtime)
    assert snapshot["routines"] == []
    assert snapshot["parameters"] == []
    assert snapshot["function_catalog"] == []
