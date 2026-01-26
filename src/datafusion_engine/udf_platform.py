"""Rust UDF platform installation helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING
from weakref import WeakSet

from datafusion import SessionContext

from datafusion_engine.expr_planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.function_factory import (
    FunctionFactoryPolicy,
    function_factory_payloads,
    install_function_factory,
)
from datafusion_engine.udf_runtime import register_rust_udfs, rust_udf_docs, rust_udf_snapshot

if TYPE_CHECKING:
    from datafusion_engine.expr_planner import ExprPlannerPolicy


@dataclass(frozen=True)
class ExtensionInstallStatus:
    """Capture installation outcome for an extension hook."""

    available: bool
    installed: bool
    error: str | None = None


@dataclass(frozen=True)
class RustUdfPlatform:
    """Snapshot of a Rust UDF platform installation."""

    snapshot: Mapping[str, object] | None
    docs: Mapping[str, object] | None
    function_factory: ExtensionInstallStatus | None
    expr_planners: ExtensionInstallStatus | None
    function_factory_policy: Mapping[str, object] | None
    expr_planner_policy: Mapping[str, object] | None


_FUNCTION_FACTORY_CTXS: WeakSet[SessionContext] = WeakSet()
_EXPR_PLANNER_CTXS: WeakSet[SessionContext] = WeakSet()


def _install_function_factory(
    ctx: SessionContext,
    *,
    enabled: bool,
    hook: Callable[[SessionContext], None] | None,
    policy: FunctionFactoryPolicy | None,
) -> tuple[ExtensionInstallStatus | None, Mapping[str, object] | None]:
    if not enabled:
        return None, None
    if ctx in _FUNCTION_FACTORY_CTXS:
        return ExtensionInstallStatus(available=True, installed=True), function_factory_payloads(
            policy
        )
    available = True
    installed = False
    error: str | None = None
    try:
        if hook is None:
            install_function_factory(ctx, policy=policy)
        else:
            hook(ctx)
        installed = True
        _FUNCTION_FACTORY_CTXS.add(ctx)
    except ImportError as exc:
        available = False
        error = str(exc)
    except (RuntimeError, TypeError, ValueError) as exc:
        error = str(exc)
    return ExtensionInstallStatus(available=available, installed=installed, error=error), function_factory_payloads(
        policy
    )


def _install_expr_planners(
    ctx: SessionContext,
    *,
    enabled: bool,
    hook: Callable[[SessionContext], None] | None,
    planner_names: Sequence[str],
) -> tuple[ExtensionInstallStatus | None, Mapping[str, object] | None]:
    if not enabled:
        return None, None
    if ctx in _EXPR_PLANNER_CTXS:
        return ExtensionInstallStatus(available=True, installed=True), expr_planner_payloads(
            planner_names
        )
    available = True
    installed = False
    error: str | None = None
    try:
        if hook is None:
            install_expr_planners(ctx, planner_names=planner_names)
        else:
            hook(ctx)
        installed = True
        _EXPR_PLANNER_CTXS.add(ctx)
    except ImportError as exc:
        available = False
        error = str(exc)
    except (RuntimeError, TypeError, ValueError) as exc:
        error = str(exc)
    return ExtensionInstallStatus(available=available, installed=installed, error=error), expr_planner_payloads(
        planner_names
    )


def install_rust_udf_platform(
    ctx: SessionContext,
    *,
    enable_udfs: bool = True,
    enable_async_udfs: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
    enable_function_factory: bool = True,
    enable_expr_planners: bool = True,
    function_factory_policy: FunctionFactoryPolicy | None = None,
    function_factory_hook: Callable[[SessionContext], None] | None = None,
    expr_planner_hook: Callable[[SessionContext], None] | None = None,
    expr_planner_names: Sequence[str] = (),
    strict: bool = True,
) -> RustUdfPlatform:
    """Install the Rust UDF platform in one step.

    Parameters
    ----------
    ctx:
        DataFusion session context to configure.
    enable_udfs:
        Whether to register Rust UDFs/UDTFs.
    enable_async_udfs:
        Whether async Rust UDFs should be registered (feature-gated in the extension).
    async_udf_timeout_ms:
        Timeout in milliseconds for async UDF execution, required when async UDFs are enabled.
    async_udf_batch_size:
        Ideal batch size for async UDFs, required when async UDFs are enabled.
    enable_function_factory:
        Whether to install the FunctionFactory hook.
    enable_expr_planners:
        Whether to install ExprPlanner hooks.
    function_factory_policy:
        Optional FunctionFactory policy overrides.
    function_factory_hook:
        Optional hook to install the FunctionFactory.
    expr_planner_hook:
        Optional hook to install ExprPlanners.
    expr_planner_names:
        ExprPlanner identifiers to install when no hook is provided.
    strict:
        When True, raise if extension installation fails.

    Returns
    -------
    RustUdfPlatform
        Installation snapshot for diagnostics.
    """
    snapshot: Mapping[str, object] | None = None
    docs: Mapping[str, object] | None = None
    if enable_udfs:
        snapshot = register_rust_udfs(
            ctx,
            enable_async=enable_async_udfs,
            async_udf_timeout_ms=async_udf_timeout_ms,
            async_udf_batch_size=async_udf_batch_size,
        )
        docs = rust_udf_docs(ctx)
    function_factory, function_factory_payload = _install_function_factory(
        ctx,
        enabled=enable_function_factory,
        hook=function_factory_hook,
        policy=function_factory_policy,
    )
    expr_planners, expr_planner_payload = _install_expr_planners(
        ctx,
        enabled=enable_expr_planners,
        hook=expr_planner_hook,
        planner_names=expr_planner_names,
    )
    if strict:
        if function_factory is not None and function_factory.error is not None:
            msg = "FunctionFactory installation failed; native extension is required."
            raise RuntimeError(msg)
        if expr_planners is not None and expr_planners.error is not None:
            msg = "ExprPlanner installation failed; native extension is required."
            raise RuntimeError(msg)
    return RustUdfPlatform(
        snapshot=snapshot,
        docs=docs,
        function_factory=function_factory,
        expr_planners=expr_planners,
        function_factory_policy=function_factory_payload,
        expr_planner_policy=expr_planner_payload,
    )


__all__ = ["ExtensionInstallStatus", "RustUdfPlatform", "install_rust_udf_platform"]
