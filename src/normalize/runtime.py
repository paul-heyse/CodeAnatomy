"""Normalize runtime helpers for DataFusion-owned execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from datafusion import SessionContext, SQLOptions
from ibis.backends import BaseBackend

from arrowdsl.core.execution_context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile, DiagnosticsSink
from datafusion_engine.sql_options import sql_options_for_profile
from ibis_engine.execution_factory import ibis_backend_from_ctx
from sqlglot_tools.optimizer import register_datafusion_dialect

if TYPE_CHECKING:
    from ibis.backends.datafusion import Backend as DataFusionBackend


@dataclass(frozen=True)
class NormalizeRuntime:
    """Runtime bundle for normalize execution."""

    execution_ctx: ExecutionContext
    ctx: SessionContext
    ibis_backend: BaseBackend
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
    register_datafusion_dialect()
    session = runtime_profile.session_context()
    backend = ibis_backend_from_ctx(ctx)
    sql_options = sql_options_for_profile(runtime_profile)
    return NormalizeRuntime(
        execution_ctx=ctx,
        ctx=session,
        ibis_backend=backend,
        sql_options=sql_options,
        runtime_profile=runtime_profile,
        diagnostics=runtime_profile.diagnostics_sink,
    )


def require_datafusion_backend(runtime: NormalizeRuntime) -> DataFusionBackend:
    """Return the Ibis DataFusion backend from a normalize runtime.

    Parameters
    ----------
    runtime:
        Normalize runtime to inspect.

    Returns
    -------
    ibis.backends.datafusion.Backend
        DataFusion-backed Ibis backend instance.

    Raises
    ------
    TypeError
        Raised when the backend is not DataFusion.
    """
    backend = runtime.ibis_backend
    if backend.name != "datafusion":
        msg = "Normalize runtime requires a DataFusion Ibis backend."
        raise TypeError(msg)
    return cast("DataFusionBackend", backend)


__all__ = ["NormalizeRuntime", "build_normalize_runtime", "require_datafusion_backend"]
