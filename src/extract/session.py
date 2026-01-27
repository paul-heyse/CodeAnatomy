"""Shared extraction session helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext
from engine.session import EngineSession
from engine.session_factory import build_engine_session

if TYPE_CHECKING:
    from datafusion_engine.runtime import SessionRuntime


@dataclass(frozen=True)
class ExtractSession:
    """Execution surfaces for extract workloads."""

    engine_session: EngineSession

    @property
    def exec_ctx(self) -> ExecutionContext:
        """Return the ExecutionContext for extract workloads."""
        return self.engine_session.ctx

    @property
    def df_ctx(self) -> SessionContext:
        """Return the DataFusion SessionContext for extract workloads.

        Raises
        ------
        ValueError
            Raised when the DataFusion SessionContext is unavailable.
        """
        ctx = self.engine_session.df_ctx()
        if ctx is None:
            msg = "DataFusion SessionContext is required for extract workloads."
            raise ValueError(msg)
        return ctx

    @property
    def session_runtime(self) -> SessionRuntime:
        """Return the DataFusion SessionRuntime for extract workloads.

        Returns
        -------
        SessionRuntime
            Planning-ready DataFusion session runtime.

        Raises
        ------
        ValueError
            Raised when the DataFusion SessionRuntime is unavailable.
        """
        runtime = self.engine_session.df_runtime()
        if runtime is None:
            msg = "DataFusion SessionRuntime is required for extract workloads."
            raise ValueError(msg)
        return runtime


def build_extract_session(ctx: ExecutionContext) -> ExtractSession:
    """Return a shared extract session for an execution context.

    Returns
    -------
    ExtractSession
        DataFusion SessionContext bound to the runtime profile.
    """
    engine_session = build_engine_session(ctx=ctx)
    return ExtractSession(engine_session=engine_session)


__all__ = ["ExtractSession", "build_extract_session"]
