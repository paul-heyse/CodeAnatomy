"""Unit tests for UDF catalog snapshots."""

from __future__ import annotations

from datafusion_engine.udf.catalog import datafusion_udf_specs
from datafusion_engine.udf.platform import ensure_rust_udfs
from tests.test_helpers.datafusion_runtime import df_ctx
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


def test_udf_specs_are_stable() -> None:
    """Keep UDF specs deterministic across snapshots."""
    ctx = df_ctx()
    snapshot = ensure_rust_udfs(ctx)
    first = datafusion_udf_specs(registry_snapshot=snapshot)
    second = datafusion_udf_specs(registry_snapshot=snapshot)
    assert first == second


def test_udf_specs_only_include_builtin_tier() -> None:
    """Ensure UDF specs only include Rust builtin tiers."""
    ctx = df_ctx()
    snapshot = ensure_rust_udfs(ctx)
    specs = datafusion_udf_specs(registry_snapshot=snapshot)
    assert all(spec.udf_tier == "builtin" for spec in specs)
