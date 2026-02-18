"""Tests for capability-first DataFusion version resolution."""

from __future__ import annotations

import pytest

from datafusion_engine.session.runtime_compile import supports_explain_analyze_level
from datafusion_engine.session.runtime_config_policies import (
    effective_datafusion_engine_major_version,
    effective_datafusion_engine_version,
)


def _capability_report(version: str) -> dict[str, object]:
    return {
        "available": True,
        "compatible": True,
        "snapshot": {
            "datafusion_version": version,
            "plugin_abi": {"major": 1, "minor": 1},
        },
    }


def test_effective_datafusion_engine_version_prefers_capability_snapshot() -> None:
    """Capability snapshots should be authoritative over wheel/package label."""
    assert effective_datafusion_engine_version(_capability_report("52.1.0")) == "52.1.0"


def test_effective_datafusion_engine_major_version_uses_capability_snapshot() -> None:
    """Major-version behavior gates should use capability-resolved engine version."""
    assert effective_datafusion_engine_major_version(_capability_report("52.1.0")) == 52


def test_supports_explain_analyze_level_uses_capability_major(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explain-analyze support should follow capability-resolved engine major."""
    monkeypatch.setattr(
        "datafusion_engine.session.runtime_compile.effective_datafusion_engine_major_version",
        lambda: 52,
    )
    assert supports_explain_analyze_level() is True
