"""Execution session model for the compute engine."""

from __future__ import annotations

from dataclasses import dataclass, field

from datafusion import SessionContext
from ibis.backends import BaseBackend

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from datafusion_engine.runtime import DataFusionRuntimeProfile
from engine.plan_policy import ExecutionSurfacePolicy
from ibis_engine.registry import IbisDatasetRegistry
from obs.diagnostics import DiagnosticsCollector


@dataclass(frozen=True)
class EngineSession:
    """Bundle core execution surfaces into a single session object."""

    ctx: ExecutionContext
    runtime_profile: RuntimeProfile
    df_profile: DataFusionRuntimeProfile | None
    ibis_backend: BaseBackend
    datasets: IbisDatasetRegistry
    diagnostics: DiagnosticsCollector | None = None
    surface_policy: ExecutionSurfacePolicy = field(default_factory=ExecutionSurfacePolicy)
    settings_hash: str | None = None

    def df_ctx(self) -> SessionContext | None:
        """Return the DataFusion SessionContext when configured.

        Returns
        -------
        datafusion.SessionContext | None
            Session context when available.
        """
        if self.df_profile is None:
            return None
        return self.df_profile.session_context()


__all__ = ["EngineSession"]
