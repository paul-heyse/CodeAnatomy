"""Test script for dual-lane compilation integration.

This script demonstrates the usage of the new dual-lane compilation features
added in Scope 4 Part 2 of the Combined Library Utilization Plan.
"""

import inspect

from datafusion import SessionContext

from datafusion_engine.bridge import (
    DualLaneCompilationResult,
    ibis_to_datafusion_dual_lane,
)
from datafusion_engine.compile_options import DataFusionCompileOptions
from engine.plan_cache import PlanCacheEntry
from ibis_engine.substrait_bridge import (
    ibis_to_substrait_bytes,
    record_substrait_gap,
    try_ibis_to_substrait_bytes,
)


def test_dual_lane_compilation_result_dataclass() -> None:
    """Test DualLaneCompilationResult dataclass structure."""
    ctx = SessionContext()
    df = ctx.sql("SELECT 1 as x")

    # Test with Substrait lane
    result_substrait = DualLaneCompilationResult(
        df=df,
        lane="substrait",
        substrait_bytes=b"mock_plan_bytes",
        fallback_reason=None,
    )

    assert result_substrait.lane == "substrait"
    assert result_substrait.substrait_bytes == b"mock_plan_bytes"
    assert result_substrait.fallback_reason is None
    # Test with SQL lane
    result_sql = DualLaneCompilationResult(
        df=df,
        lane="sql",
        substrait_bytes=None,
        fallback_reason="Substrait not available",
    )

    assert result_sql.lane == "sql"
    assert result_sql.substrait_bytes is None
    assert result_sql.fallback_reason == "Substrait not available"


def test_compile_options_substrait_fields() -> None:
    """Test new Substrait-related fields in DataFusionCompileOptions."""
    # Test default values
    opts_default = DataFusionCompileOptions()
    assert opts_default.prefer_substrait is False
    assert opts_default.record_substrait_gaps is False
    # Test with Substrait enabled
    opts_enabled = DataFusionCompileOptions(
        prefer_substrait=True,
        record_substrait_gaps=True,
    )
    assert opts_enabled.prefer_substrait is True
    assert opts_enabled.record_substrait_gaps is True


def test_plan_cache_entry_compilation_lane() -> None:
    """Test compilation_lane field in PlanCacheEntry."""
    entry = PlanCacheEntry(
        plan_hash="hash123",
        profile_hash="profile456",
        plan_bytes=b"plan_data",
        compilation_lane="substrait",
    )

    assert entry.compilation_lane == "substrait"
    # Test optional field
    entry_no_lane = PlanCacheEntry(
        plan_hash="hash789",
        profile_hash="profile012",
        plan_bytes=b"plan_data",
    )

    assert entry_no_lane.compilation_lane is None


def test_dual_lane_function_imports() -> None:
    """Test that all necessary functions and classes are importable."""
    # Verify function signature
    sig = inspect.signature(ibis_to_datafusion_dual_lane)
    params = list(sig.parameters.keys())

    assert "expr" in params
    assert "backend" in params
    assert "ctx" in params
    assert "options" in params


def test_substrait_bridge_imports() -> None:
    """Test that substrait_bridge functions are available."""
    assert callable(ibis_to_substrait_bytes)
    assert callable(try_ibis_to_substrait_bytes)
    assert callable(record_substrait_gap)
