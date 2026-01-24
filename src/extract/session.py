"""Shared extraction session helpers."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from engine.session import EngineSession
from engine.session_factory import build_engine_session


@dataclass(frozen=True)
class ExtractSession:
    """Execution surfaces for extract workloads."""

    engine_session: EngineSession

    @property
    def exec_ctx(self) -> ExecutionContext:
        """Return the ExecutionContext for extract workloads."""
        return self.engine_session.ctx

    @property
    def ibis_backend(self) -> BaseBackend:
        """Return the Ibis backend for extract workloads."""
        return self.engine_session.ibis_backend

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


def build_extract_session(ctx: ExecutionContext) -> ExtractSession:
    """Return a shared extract session for an execution context.

    Returns
    -------
    ExtractSession
        DataFusion SessionContext plus Ibis backend bound to the runtime profile.
    """
    engine_session = build_engine_session(ctx=ctx)
    return ExtractSession(engine_session=engine_session)


__all__ = ["ExtractSession", "build_extract_session"]
