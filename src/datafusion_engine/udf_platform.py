"""Rust UDF platform installation for planning-critical extensions.

This module installs the Rust UDF platform as a core planning feature.
Planner extensions (Rust UDFs, ExprPlanner, FunctionFactory, RelationPlanner,
and table functions) are treated as planning-critical components that must
be installed before any plan-bundle construction.

The platform installation pattern:

    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf_platform import (
        RustUdfPlatformOptions,
        install_rust_udf_platform,
    )

    ctx = DataFusionRuntimeProfile().session_context()
    options = RustUdfPlatformOptions(
        enable_udfs=True,
        enable_function_factory=True,
        enable_expr_planners=True,
        expr_planner_names=("codeanatomy_domain",),
        strict=True,
    )
    install_rust_udf_platform(ctx, options=options)

All DataFusion execution facades automatically install the platform in
`__post_init__` to ensure extensions are available before plan operations.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast
from weakref import WeakSet

from datafusion import SessionContext

from datafusion_engine.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.expr_planner import expr_planner_payloads, install_expr_planners
from datafusion_engine.function_factory import (
    FunctionFactoryPolicy,
    function_factory_payloads,
    function_factory_policy_from_snapshot,
    install_function_factory,
)
from datafusion_engine.udf_catalog import rewrite_tag_index
from datafusion_engine.udf_runtime import (
    register_rust_udfs,
    rust_udf_docs,
    rust_udf_snapshot_hash,
)


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
    snapshot_hash: str | None
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    docs: Mapping[str, object] | None
    function_factory: ExtensionInstallStatus | None
    expr_planners: ExtensionInstallStatus | None
    function_factory_policy: Mapping[str, object] | None
    expr_planner_policy: Mapping[str, object] | None


@dataclass(frozen=True)
class RustUdfPlatformOptions:
    """Configuration for installing the Rust UDF platform."""

    enable_udfs: bool = True
    enable_async_udfs: bool = False
    async_udf_timeout_ms: int | None = None
    async_udf_batch_size: int | None = None
    enable_function_factory: bool = True
    enable_expr_planners: bool = True
    function_factory_policy: FunctionFactoryPolicy | None = None
    function_factory_hook: Callable[[SessionContext], None] | None = None
    expr_planner_hook: Callable[[SessionContext], None] | None = None
    expr_planner_names: Sequence[str] = ()
    strict: bool = True


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
    return ExtensionInstallStatus(
        available=available, installed=installed, error=error
    ), function_factory_payloads(policy)


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
    return ExtensionInstallStatus(
        available=available, installed=installed, error=error
    ), expr_planner_payloads(planner_names)


def install_rust_udf_platform(
    ctx: SessionContext,
    *,
    options: RustUdfPlatformOptions | None = None,
) -> RustUdfPlatform:
    """Install planning-critical extension platform before plan-bundle construction.

    This function installs the Rust UDF platform as a core planning feature.
    Planner extensions (Rust UDFs, ExprPlanner, FunctionFactory) must be
    installed before any DataFusion plan-bundle construction to ensure:

    1. Required UDFs can be derived from logical plans
    2. Domain syntax is routed through ExprPlanner
    3. Function rewrites are applied via FunctionFactory
    4. Table functions and relation planners are available

    This replaces legacy UDF enforcement paths with DataFusion-native
    extension points.

    Parameters
    ----------
    ctx:
        DataFusion session context to configure.
    options:
        Optional platform installation configuration.

    Returns
    -------
    RustUdfPlatform
        Installation snapshot for diagnostics.

    Raises
    ------
    RuntimeError
        Raised when strict installation is enabled and extensions fail to install.
    """
    resolved = options or RustUdfPlatformOptions()
    snapshot: Mapping[str, object] | None = None
    docs: Mapping[str, object] | None = None
    snapshot_hash: str | None = None
    rewrite_tags: tuple[str, ...] = ()
    if resolved.enable_udfs:
        snapshot = register_rust_udfs(
            ctx,
            enable_async=resolved.enable_async_udfs,
            async_udf_timeout_ms=resolved.async_udf_timeout_ms,
            async_udf_batch_size=resolved.async_udf_batch_size,
        )
        snapshot_hash = rust_udf_snapshot_hash(snapshot)
        tag_index = rewrite_tag_index(snapshot)
        rewrite_tags = tuple(sorted(tag_index))
        docs_value = snapshot.get("documentation") if isinstance(snapshot, Mapping) else None
        docs = cast("Mapping[str, object] | None", docs_value)
        if docs is None:
            docs = rust_udf_docs(ctx)
    planner_names = tuple(resolved.expr_planner_names)
    derived_planners = domain_planner_names_from_snapshot(snapshot)
    if resolved.enable_expr_planners and resolved.expr_planner_hook is None:
        if planner_names:
            planner_names = tuple(dict.fromkeys((*planner_names, *derived_planners)))
        else:
            planner_names = derived_planners
    function_factory_policy = resolved.function_factory_policy
    if function_factory_policy is None and snapshot is not None:
        function_factory_policy = function_factory_policy_from_snapshot(
            snapshot,
            allow_async=resolved.enable_async_udfs,
        )
    function_factory, function_factory_payload = _install_function_factory(
        ctx,
        enabled=resolved.enable_function_factory,
        hook=resolved.function_factory_hook,
        policy=function_factory_policy,
    )
    expr_planners, expr_planner_payload = _install_expr_planners(
        ctx,
        enabled=resolved.enable_expr_planners,
        hook=resolved.expr_planner_hook,
        planner_names=planner_names,
    )
    if resolved.strict:
        if function_factory is not None and function_factory.error is not None:
            msg = "FunctionFactory installation failed; native extension is required."
            raise RuntimeError(msg)
        if expr_planners is not None and expr_planners.error is not None:
            msg = "ExprPlanner installation failed; native extension is required."
            raise RuntimeError(msg)
    return RustUdfPlatform(
        snapshot=snapshot,
        snapshot_hash=snapshot_hash,
        rewrite_tags=rewrite_tags,
        domain_planner_names=planner_names,
        docs=docs,
        function_factory=function_factory,
        expr_planners=expr_planners,
        function_factory_policy=function_factory_payload,
        expr_planner_policy=expr_planner_payload,
    )


__all__ = [
    "ExtensionInstallStatus",
    "RustUdfPlatform",
    "RustUdfPlatformOptions",
    "install_rust_udf_platform",
]
