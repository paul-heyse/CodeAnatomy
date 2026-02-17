"""Execution session model for the compute engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.materialize_policy import MaterializationPolicy
from datafusion_engine.session.facade import DataFusionExecutionFacade
from datafusion_engine.session.runtime_session import session_runtime_for_context
from extraction.engine_runtime import EngineRuntime

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_session import SessionRuntime
from datafusion_engine.dataset.registry import DatasetCatalog
from obs.diagnostics import DiagnosticsCollector


@dataclass(frozen=True)
class EngineSession:
    """Bundle core execution surfaces into a single session object."""

    engine_runtime: EngineRuntime
    datasets: DatasetCatalog
    diagnostics: DiagnosticsCollector | None = None
    surface_policy: MaterializationPolicy = field(default_factory=MaterializationPolicy)
    settings_hash: str | None = None
    runtime_profile_hash: str | None = None

    @property
    def datafusion_profile(self) -> DataFusionRuntimeProfile:
        """Return the DataFusion runtime profile when configured."""
        return self.engine_runtime.datafusion_profile

    def df_ctx(self) -> SessionContext:
        """Return the DataFusion SessionContext when configured.

        Returns:
        -------
        datafusion.SessionContext | None
            Session context when available.
        """
        if self.engine_runtime.session_context is not None:
            return self.engine_runtime.session_context
        return self.engine_runtime.datafusion_profile.session_runtime().ctx

    def df_runtime(self) -> SessionRuntime:
        """Return the DataFusion SessionRuntime when configured.

        Returns:
        -------
        SessionRuntime | None
            Session runtime when available.
        """
        ctx = self.engine_runtime.session_context
        if ctx is not None:
            runtime = session_runtime_for_context(self.engine_runtime.datafusion_profile, ctx)
            if runtime is not None:
                return runtime
        return self.engine_runtime.datafusion_profile.session_runtime()

    def datafusion_facade(self) -> DataFusionExecutionFacade:
        """Return a DataFusion execution facade when configured.

        Returns:
        -------
        DataFusionExecutionFacade | None
            Execution facade when available.
        """
        session_runtime = self.df_runtime()
        return DataFusionExecutionFacade(
            ctx=session_runtime.ctx,
            runtime_profile=self.engine_runtime.datafusion_profile,
        )


__all__ = ["EngineSession"]
