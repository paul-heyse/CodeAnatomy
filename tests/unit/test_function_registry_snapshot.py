"""Unit tests for UDF catalog snapshots."""

from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.udf_catalog import datafusion_udf_specs
from datafusion_engine.udf_runtime import register_rust_udfs


def test_udf_specs_are_stable() -> None:
    """Keep UDF specs deterministic across snapshots."""
    ctx = SessionContext()
    snapshot = register_rust_udfs(ctx)
    first = datafusion_udf_specs(registry_snapshot=snapshot)
    second = datafusion_udf_specs(registry_snapshot=snapshot)
    assert first == second


def test_udf_specs_only_include_builtin_tier() -> None:
    """Ensure UDF specs only include Rust builtin tiers."""
    ctx = SessionContext()
    snapshot = register_rust_udfs(ctx)
    specs = datafusion_udf_specs(registry_snapshot=snapshot)
    assert all(spec.udf_tier == "builtin" for spec in specs)
