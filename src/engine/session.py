"""Execution session model for the compute engine.

DEPRECATION NOTICE: Ibis backend coupling in EngineSession and DataFusionExecutionFacade
is deprecated. Use DataFusion-native builder functions instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from datafusion import SessionContext
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.execution_facade import DataFusionExecutionFacade
from engine.plan_policy import ExecutionSurfacePolicy
from engine.runtime import EngineRuntime

if TYPE_CHECKING:
    from arrowdsl.core.runtime_profiles import RuntimeProfile
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from sqlglot_tools.bridge import IbisCompilerBackend
from ibis_engine.registry import IbisDatasetRegistry
from obs.diagnostics import DiagnosticsCollector


@dataclass(frozen=True)
class EngineSession:
    """Bundle core execution surfaces into a single session object."""

    ctx: ExecutionContext
    engine_runtime: EngineRuntime
    ibis_backend: BaseBackend
    datasets: IbisDatasetRegistry
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

        DEPRECATION NOTICE: The ibis_backend parameter passed to DataFusionExecutionFacade
        is deprecated. Future versions will not require Ibis backend configuration.

        Returns
        -------
        DataFusionExecutionFacade | None
            Facade bound to the DataFusion session when available.
        """
        if self.engine_runtime.datafusion_profile is None:
            return None
        # DEPRECATED: ibis_backend parameter is deprecated, will be removed in future version
        return DataFusionExecutionFacade(
            ctx=self.engine_runtime.datafusion_profile.session_context(),
            runtime_profile=self.engine_runtime.datafusion_profile,
            ibis_backend=cast("IbisCompilerBackend", self.ibis_backend),  # DEPRECATED
        )


__all__ = ["EngineSession"]
