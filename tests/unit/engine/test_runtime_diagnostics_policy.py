"""Tests for diagnostics policy application."""

from __future__ import annotations

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from engine.runtime import build_engine_runtime
from relspec.pipeline_policy import DiagnosticsPolicy


def test_diagnostics_policy_applies_semantic_quality_flag() -> None:
    """Diagnostics policy should propagate semantic quality flag."""
    profile = DataFusionRuntimeProfile()
    policy = DiagnosticsPolicy(emit_semantic_quality_diagnostics=False)
    runtime = build_engine_runtime(runtime_profile=profile, diagnostics_policy=policy)
    assert runtime.datafusion_profile.diagnostics.emit_semantic_quality_diagnostics is False
