"""Integration tests for ExprPlanner hooks and named args."""

from __future__ import annotations

import importlib.util

import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile
from obs.diagnostics import DiagnosticsCollector

datafusion = pytest.importorskip("datafusion")


@pytest.mark.integration
def test_expr_planner_install_records_event() -> None:
    """Record ExprPlanner installation attempts."""
    sink = DiagnosticsCollector()
    profile = DataFusionRuntimeProfile(
        diagnostics_sink=sink,
        enable_expr_planners=True,
        expr_planner_names=("codeintel_ops",),
    )
    has_extension = importlib.util.find_spec("datafusion_ext") is not None
    if not has_extension:
        with pytest.raises(RuntimeError, match="ExprPlanner installation failed"):
            profile.session_context()
        events = sink.artifacts_snapshot().get("datafusion_expr_planners_v1", [])
        assert events
        assert events[-1].get("installed") is False
        return
    ctx = profile.session_context()
    assert ctx is not None
    events = sink.artifacts_snapshot().get("datafusion_expr_planners_v1", [])
    assert events
    assert events[-1].get("installed") is True


@pytest.mark.integration
def test_named_argument_sql_is_gated() -> None:
    """Gate named-argument SQL calls when planners are unavailable."""
    ctx = datafusion.SessionContext()
    expected_errors: tuple[type[Exception], ...] = (RuntimeError, TypeError, ValueError)
    with pytest.raises(expected_errors, match="named arguments"):
        ctx.sql("SELECT rpad(str => 'a', n => 3, padding_str => 'x')").to_arrow_table()
