"""Normalize runtime helpers for DataFusion-owned execution."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext, SQLOptions

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.diagnostics import DiagnosticsSink
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.sql_options import sql_options_for_profile


@dataclass(frozen=True)
class NormalizeRuntime:
    """Runtime bundle for normalize execution."""

    execution_ctx: ExecutionContext
    ctx: SessionContext
    sql_options: SQLOptions
    runtime_profile: DataFusionRuntimeProfile
    diagnostics: DiagnosticsSink | None


def build_normalize_runtime(ctx: ExecutionContext) -> NormalizeRuntime:
    """Build the normalize runtime from an execution context.

    Parameters
    ----------
    ctx:
        Execution context with a DataFusion runtime profile.

    Returns
    -------
    NormalizeRuntime
        Normalize runtime with DataFusion session and Ibis backend.

    Raises
    ------
    ValueError
        Raised when the execution context lacks a DataFusion profile.
    """
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        msg = "Normalize requires a DataFusion runtime profile."
        raise ValueError(msg)
    session = runtime_profile.session_context()
    sql_options = sql_options_for_profile(runtime_profile)
    return NormalizeRuntime(
        execution_ctx=ctx,
        ctx=session,
        sql_options=sql_options,
        runtime_profile=runtime_profile,
        diagnostics=runtime_profile.diagnostics_sink,
    )


__all__ = ["NormalizeRuntime", "build_normalize_runtime"]
