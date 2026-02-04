"""Integration tests for FunctionFactory installation."""

from __future__ import annotations

import importlib.util

import pytest

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DiagnosticsConfig,
    FeatureGatesConfig,
)
from tests.test_helpers.diagnostics import diagnostic_profile
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


@pytest.mark.integration
def test_function_factory_records_installation_result() -> None:
    """Record FunctionFactory installation success or failure."""
    profile, sink = diagnostic_profile(
        profile_factory=lambda diagnostics: DataFusionRuntimeProfile(
            diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
            features=FeatureGatesConfig(enable_function_factory=True),
        )
    )
    has_extension = importlib.util.find_spec("datafusion_ext") is not None
    if not has_extension:
        with pytest.raises(RuntimeError, match="FunctionFactory installation failed"):
            profile.session_context()
        events = sink.artifacts_snapshot().get("datafusion_function_factory_v1", [])
        assert events
        assert events[-1].get("installed") is False
        return
    ctx = profile.session_context()
    assert ctx is not None
    events = sink.artifacts_snapshot().get("datafusion_function_factory_v1", [])
    assert events
    assert events[-1].get("installed") is True
