"""Pipeline policy controls for relspec execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.tables.param import ParamTablePolicy


@dataclass(frozen=True)
class DiagnosticsPolicy(FingerprintableConfig):
    """Diagnostics capture policy for pipeline execution."""

    capture_datafusion_metrics: bool = True
    capture_datafusion_traces: bool = True
    capture_datafusion_explains: bool = True
    explain_analyze: bool = True
    explain_analyze_level: str | None = "summary"
    emit_kernel_lane_diagnostics: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for diagnostics policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing diagnostics policy settings.
        """
        return {
            "capture_datafusion_metrics": self.capture_datafusion_metrics,
            "capture_datafusion_traces": self.capture_datafusion_traces,
            "capture_datafusion_explains": self.capture_datafusion_explains,
            "explain_analyze": self.explain_analyze,
            "explain_analyze_level": self.explain_analyze_level,
            "emit_kernel_lane_diagnostics": self.emit_kernel_lane_diagnostics,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for diagnostics policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class PipelinePolicy(FingerprintableConfig):
    """Centralized pipeline policy for execution and diagnostics."""

    param_table_policy: ParamTablePolicy = field(default_factory=ParamTablePolicy)
    diagnostics: DiagnosticsPolicy = field(default_factory=DiagnosticsPolicy)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the pipeline policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing pipeline policy settings.
        """
        return {
            "param_table_policy": self.param_table_policy.fingerprint(),
            "diagnostics": self.diagnostics.fingerprint(),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the pipeline policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


__all__ = ["DiagnosticsPolicy", "PipelinePolicy"]
