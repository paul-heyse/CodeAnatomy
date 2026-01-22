"""Shared extraction session helpers."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig


@dataclass(frozen=True)
class ExtractSession:
    """Execution surfaces for extract workloads."""

    exec_ctx: ExecutionContext
    df_profile: DataFusionRuntimeProfile
    df_ctx: SessionContext
    ibis_backend: BaseBackend


def build_extract_session(ctx: ExecutionContext) -> ExtractSession:
    """Return a shared extract session for an execution context.

    Returns
    -------
    ExtractSession
        DataFusion SessionContext plus Ibis backend bound to the runtime profile.
    """
    df_profile = ctx.runtime.datafusion or DataFusionRuntimeProfile()
    df_ctx = df_profile.session_context()
    backend = build_backend(IbisBackendConfig(datafusion_profile=df_profile))
    return ExtractSession(
        exec_ctx=ctx,
        df_profile=df_profile,
        df_ctx=df_ctx,
        ibis_backend=backend,
    )


__all__ = ["ExtractSession", "build_extract_session"]
