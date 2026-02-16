# ruff: noqa: D103, INP001
"""Tests for runtime metrics snapshot capability surface."""

from __future__ import annotations

from datafusion_engine.extensions import runtime_capabilities


def test_runtime_metrics_collector_symbol_exists() -> None:
    assert hasattr(runtime_capabilities, "collect_runtime_execution_metrics")
