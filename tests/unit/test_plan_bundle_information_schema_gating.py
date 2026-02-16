"""Tests for information_schema snapshot gating in plan bundles."""

from __future__ import annotations

import pytest

from datafusion_engine.plan import bundle_artifact as plan_bundle
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, FeatureGatesConfig


def test_information_schema_snapshot_skips_udf_catalog_when_udfs_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure plan snapshots do not query routine metadata with UDFs disabled."""
    profile = DataFusionRuntimeProfile(features=FeatureGatesConfig(enable_udfs=False))
    runtime = profile.session_runtime()
    ctx = runtime.ctx

    class _FakeIntrospector:
        def __init__(self, _ctx: object, *, sql_options: object | None = None) -> None:
            _ = (_ctx, sql_options)

        @staticmethod
        def tables_snapshot() -> list[dict[str, object]]:
            return [{"table_name": "events"}]

        @staticmethod
        def table_definition(_name: str) -> str:
            return "CREATE TABLE events (id BIGINT)"

        @staticmethod
        def settings_snapshot() -> list[dict[str, object]]:
            return []

        @staticmethod
        def schemata_snapshot() -> list[dict[str, object]]:
            return []

        @staticmethod
        def columns_snapshot() -> list[dict[str, object]]:
            return []

        @staticmethod
        def routines_snapshot() -> list[dict[str, object]]:
            msg = "routines_snapshot should be gated off when enable_udfs=False"
            raise AssertionError(msg)

        @staticmethod
        def parameters_snapshot() -> list[dict[str, object]]:
            msg = "parameters_snapshot should be gated off when enable_udfs=False"
            raise AssertionError(msg)

        @staticmethod
        def function_catalog_snapshot(
            *,
            include_parameters: bool = False,
        ) -> list[dict[str, object]]:
            _ = include_parameters
            msg = "function_catalog_snapshot should be gated off when enable_udfs=False"
            raise AssertionError(msg)

    monkeypatch.setattr(plan_bundle, "SchemaIntrospector", _FakeIntrospector)

    snapshot_fn = plan_bundle._information_schema_snapshot  # noqa: SLF001
    snapshot = snapshot_fn(ctx, session_runtime=runtime)
    assert snapshot["routines"] == []
    assert snapshot["parameters"] == []
    assert snapshot["function_catalog"] == []
