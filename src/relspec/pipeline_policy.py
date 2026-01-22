"""Pipeline policy controls for relspec execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from arrowdsl.kernel.registry import KernelLane
from datafusion_engine.compile_options import DataFusionSqlPolicy
from ibis_engine.param_tables import ParamTablePolicy
from relspec.list_filter_gate import ListFilterGatePolicy

if TYPE_CHECKING:
    from relspec.config import RelspecConfig
from relspec.policies import PolicyRegistry


@dataclass(frozen=True)
class KernelLanePolicy:
    """Policy for permitted kernel lanes and enforcement behavior."""

    allowed: tuple[KernelLane, ...] = (
        KernelLane.DF_UDF,
        KernelLane.BUILTIN,
    )
    on_violation: str = "warn"


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
    """Centralized pipeline policy for rule execution and diagnostics."""

    policy_registry: PolicyRegistry = field(default_factory=PolicyRegistry)
    param_table_policy: ParamTablePolicy = field(default_factory=ParamTablePolicy)
    list_filter_gate_policy: ListFilterGatePolicy = field(default_factory=ListFilterGatePolicy)
    datafusion_sql_policy: DataFusionSqlPolicy = field(default_factory=DataFusionSqlPolicy)
    kernel_lanes: KernelLanePolicy = field(default_factory=KernelLanePolicy)
    diagnostics: DiagnosticsPolicy = field(default_factory=DiagnosticsPolicy)

    def relspec_config(self) -> RelspecConfig:
        """Return the rule registry config derived from this policy.

        Returns
        -------
        RelspecConfig
            Configuration payload for relspec registry wiring.
        """
        from relspec.config import RelspecConfig

        return RelspecConfig(
            param_table_policy=self.param_table_policy,
            list_filter_gate_policy=self.list_filter_gate_policy,
            kernel_lane_policy=self.kernel_lanes,
        )


__all__ = ["DiagnosticsPolicy", "KernelLanePolicy", "PipelinePolicy"]
