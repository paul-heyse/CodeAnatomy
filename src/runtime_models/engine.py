"""Runtime validation model for [engine] config."""

from __future__ import annotations

from typing import Literal

from runtime_models.base import RuntimeBase


class EngineConfigRuntime(RuntimeBase):
    """Validated engine configuration."""

    profile: Literal["small", "medium", "large"] = "medium"
    rulepack_profile: Literal["Default", "LowLatency", "Replay", "Strict"] = "Default"
    compliance_capture: bool = False
    rule_tracing: bool = False
    plan_preview: bool = False
    tracing_preset: Literal["Maximal", "MaximalNoData", "ProductionLean"] = "ProductionLean"
    instrument_object_store: bool = False


__all__ = ["EngineConfigRuntime"]
