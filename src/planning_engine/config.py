"""Engine-native configuration types.

Replace Hamilton-specific config types with engine-native equivalents.
These types are used by the CLI, build orchestrator, and public API.
"""

from __future__ import annotations

from typing import Literal

import msgspec

type EngineProfile = Literal["small", "medium", "large"]
type RulepackProfile = Literal["Default", "LowLatency", "Replay", "Strict"]
type TracingPreset = Literal["Maximal", "MaximalNoData", "ProductionLean"]


class EngineConfigSpec(msgspec.Struct, frozen=True):
    """Engine configuration from config file [engine] section.

    Replaces HamiltonConfigSpec. Controls engine execution behavior
    including profiling, rule packs, compliance, and tracing.
    """

    profile: EngineProfile = "medium"
    rulepack_profile: RulepackProfile = "Default"
    compliance_capture: bool = False
    rule_tracing: bool = False
    plan_preview: bool = False
    tracing_preset: TracingPreset = "ProductionLean"
    instrument_object_store: bool = False


class EngineExecutionOptions(msgspec.Struct, frozen=True):
    """Runtime execution options for a single engine invocation.

    Bundles the engine profile, rulepack profile, and optional
    config objects needed by the build orchestrator.
    """

    engine_profile: EngineProfile = "medium"
    rulepack_profile: RulepackProfile = "Default"
    runtime_config: object | None = None
    extraction_config: object | None = None
    incremental_config: object | None = None


class CompiledPlanSummary(msgspec.Struct, frozen=True):
    """Summary of a compiled engine plan.

    Mirrors the plan summary artifact fields for observability
    and diagnostics reporting.
    """

    spec_hash: str
    view_count: int
    join_edge_count: int
    rule_intent_count: int
    rulepack_profile: RulepackProfile
    input_relation_count: int
    output_target_count: int


__all__ = [
    "CompiledPlanSummary",
    "EngineConfigSpec",
    "EngineExecutionOptions",
    "EngineProfile",
]
