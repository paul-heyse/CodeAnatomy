"""Factory helpers for Ibis backends and execution contexts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.execution_context import ExecutionContext
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext

if TYPE_CHECKING:
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel


def backend_config_from_ctx(ctx: ExecutionContext) -> IbisBackendConfig:
    """Build an Ibis backend config from an execution context.

    Returns
    -------
    IbisBackendConfig
        Backend config derived from the execution context runtime.
    """
    runtime = ctx.runtime
    return IbisBackendConfig(
        datafusion_profile=runtime.datafusion,
        fuse_selects=runtime.ibis_fuse_selects,
        default_limit=runtime.ibis_default_limit,
        default_dialect=runtime.ibis_default_dialect,
        interactive=runtime.ibis_interactive,
    )


def ibis_backend_from_ctx(ctx: ExecutionContext) -> BaseBackend:
    """Return a configured Ibis backend for the provided execution context.

    Returns
    -------
    ibis.backends.BaseBackend
        Configured backend instance.
    """
    return build_backend(backend_config_from_ctx(ctx))


def datafusion_facade_from_ctx(
    ctx: ExecutionContext,
    *,
    backend: BaseBackend | None = None,
) -> DataFusionExecutionFacade | None:
    """Return a DataFusionExecutionFacade for the provided context.

    Returns
    -------
    DataFusionExecutionFacade | None
        Facade bound to the DataFusion session when configured.
    """
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        return None
    backend = ibis_backend_from_ctx(ctx) if backend is None else backend
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
    from sqlglot_tools.bridge import IbisCompilerBackend

    return DataFusionExecutionFacade(
        ctx=runtime_profile.session_context(),
        runtime_profile=runtime_profile,
        ibis_backend=cast("IbisCompilerBackend", backend),
    )


def ibis_execution_from_ctx(
    ctx: ExecutionContext,
    *,
    backend: BaseBackend | None = None,
    params: Mapping[IbisValue, object] | None = None,
    execution_policy: AdapterExecutionPolicy | None = None,
    execution_label: ExecutionLabel | None = None,
) -> IbisExecutionContext:
    """Return an execution context with a configured Ibis backend.

    Returns
    -------
    IbisExecutionContext
        Execution context bound to a configured backend.
    """
    backend = ibis_backend_from_ctx(ctx) if backend is None else backend
    return IbisExecutionContext(
        ctx=ctx,
        ibis_backend=backend,
        params=params,
        execution_policy=execution_policy,
        execution_label=execution_label,
    )


def ibis_execution_from_profile(profile: object) -> IbisExecutionContext:
    """Return an Ibis execution context for a DataFusion runtime profile.

    This helper is intentionally light on types to avoid import cycles.

    Returns
    -------
    IbisExecutionContext
        Execution context bound to the provided profile.

    Raises
    ------
    TypeError
        Raised when the input is not a DataFusionRuntimeProfile.
    """
    from arrowdsl.core.runtime_profiles import runtime_profile_factory
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    if not isinstance(profile, DataFusionRuntimeProfile):
        msg = f"Expected DataFusionRuntimeProfile, got {type(profile)!r}."
        raise TypeError(msg)
    runtime_profile = runtime_profile_factory("default").with_datafusion(profile)
    ctx = ExecutionContext(runtime=runtime_profile)
    backend = ibis_backend_from_ctx(ctx)
    return IbisExecutionContext(ctx=ctx, ibis_backend=backend)


def ibis_backend_from_profile(profile: object) -> BaseBackend:
    """Return a configured Ibis backend for a DataFusion runtime profile.

    Returns
    -------
    ibis.backends.BaseBackend
        Configured backend instance.

    Raises
    ------
    TypeError
        Raised when the input is not a DataFusionRuntimeProfile.
    """
    from arrowdsl.core.runtime_profiles import runtime_profile_factory
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    if not isinstance(profile, DataFusionRuntimeProfile):
        msg = f"Expected DataFusionRuntimeProfile, got {type(profile)!r}."
        raise TypeError(msg)
    runtime_profile = runtime_profile_factory("default").with_datafusion(profile)
    ctx = ExecutionContext(runtime=runtime_profile)
    return ibis_backend_from_ctx(ctx)


__all__ = [
    "backend_config_from_ctx",
    "datafusion_facade_from_ctx",
    "ibis_backend_from_ctx",
    "ibis_backend_from_profile",
    "ibis_execution_from_ctx",
    "ibis_execution_from_profile",
]
