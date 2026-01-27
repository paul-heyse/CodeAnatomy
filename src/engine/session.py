"""Execution session model for the compute engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.execution_facade import DataFusionExecutionFacade
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime import EngineRuntime

if TYPE_CHECKING:
    from arrowdsl.core.runtime_profiles import RuntimeProfile
    from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.dataset_registry import DatasetCatalog
from obs.diagnostics import DiagnosticsCollector


@dataclass(frozen=True)
class EngineSession:
    """Bundle core execution surfaces into a single session object."""

    ctx: ExecutionContext
    engine_runtime: EngineRuntime
    datasets: DatasetCatalog
    diagnostics: DiagnosticsCollector | None = None
    surface_policy: ExecutionSurfacePolicy = field(default_factory=ExecutionSurfacePolicy)
    settings_hash: str | None = None
    runtime_profile_hash: str | None = None

    @property
    def runtime_profile(self) -> RuntimeProfile:
        """Return the runtime profile for the session."""
        return self.engine_runtime.runtime_profile

    @property
    def datafusion_profile(self) -> DataFusionRuntimeProfile | None:
        """Return the DataFusion runtime profile when configured."""
        return self.engine_runtime.datafusion_profile

    def df_ctx(self) -> SessionContext | None:
        """Return the DataFusion SessionContext when configured.

        Returns
        -------
        datafusion.SessionContext | None
            Session context when available.
        """
        if self.engine_runtime.datafusion_profile is None:
            return None
        return self.engine_runtime.datafusion_profile.session_context()

    def datafusion_facade(self) -> DataFusionExecutionFacade | None:
        """Return a DataFusion execution facade when configured.

        Returns
        -------
        DataFusionExecutionFacade | None
            Execution facade when available.
        """
        if self.engine_runtime.datafusion_profile is None:
            return None
        return DataFusionExecutionFacade(
            ctx=self.engine_runtime.datafusion_profile.session_context(),
            runtime_profile=self.engine_runtime.datafusion_profile,
        )


__all__ = ["EngineSession"]
