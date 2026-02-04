"""Integration tests for ExprPlanner hooks and named args."""

from __future__ import annotations

import importlib.util

import pytest

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DiagnosticsConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from datafusion_engine.sql.guard import safe_sql
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.diagnostics import diagnostic_profile
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


@pytest.mark.integration
def test_expr_planner_install_records_event() -> None:
    """Record ExprPlanner installation attempts."""
    profile, sink = diagnostic_profile(
        profile_factory=lambda diagnostics: DataFusionRuntimeProfile(
            diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
            features=FeatureGatesConfig(enable_expr_planners=True),
            policies=PolicyBundleConfig(expr_planner_names=("codeintel_ops",)),
        )
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
    profile = df_profile()
    ctx = profile.session_context()
    with pytest.raises(ValueError, match="named arguments"):
        safe_sql(
            ctx,
            "SELECT rpad(str => 'a', n => 3, padding_str => 'x')",
            runtime_profile=profile,
        ).to_arrow_table()
