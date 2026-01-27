"""Pipeline policy controls for relspec execution."""

from __future__ import annotations

from dataclasses import dataclass, field

from datafusion_engine.param_tables import ParamTablePolicy


@dataclass(frozen=True)
class DiagnosticsPolicy:
    """Diagnostics capture policy for pipeline execution."""

    capture_datafusion_metrics: bool = True
    capture_datafusion_traces: bool = True
    capture_datafusion_explains: bool = True
    explain_analyze: bool = True
    explain_analyze_level: str | None = "summary"
    emit_kernel_lane_diagnostics: bool = True


@dataclass(frozen=True)
class PipelinePolicy:
    """Centralized pipeline policy for execution and diagnostics."""

    param_table_policy: ParamTablePolicy = field(default_factory=ParamTablePolicy)
    diagnostics: DiagnosticsPolicy = field(default_factory=DiagnosticsPolicy)


__all__ = ["DiagnosticsPolicy", "PipelinePolicy"]
