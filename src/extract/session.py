"""Shared extraction session helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext

from engine.runtime_profile import RuntimeProfileSpec
from engine.session import EngineSession
from engine.session_factory import build_engine_session

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import SessionRuntime


@dataclass(frozen=True)
class ExtractSession:
    """Execution surfaces for extract workloads."""

    engine_session: EngineSession

    @property
    def df_ctx(self) -> SessionContext:
        """Return the DataFusion SessionContext for extract workloads.

        Returns:
        -------
        SessionContext
            DataFusion SessionContext for extract workloads.
        """
        return self.engine_session.df_ctx()

    @property
    def session_runtime(self) -> SessionRuntime:
        """Return the DataFusion SessionRuntime for extract workloads.

        Returns:
        -------
        SessionRuntime
            Planning-ready DataFusion session runtime.

        """
        return self.engine_session.df_runtime()


def build_extract_session(runtime_spec: RuntimeProfileSpec) -> ExtractSession:
    """Return a shared extract session for a runtime profile spec.

    Returns:
    -------
    ExtractSession
        DataFusion SessionContext bound to the runtime profile.
    """
    engine_session = build_engine_session(runtime_spec=runtime_spec)
    return ExtractSession(engine_session=engine_session)


__all__ = ["ExtractSession", "build_extract_session"]
