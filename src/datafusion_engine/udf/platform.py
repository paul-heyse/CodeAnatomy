"""Rust UDF platform installation for planning-critical extensions.

This module installs the Rust UDF platform as a core planning feature.
Planner extensions (Rust UDFs, ExprPlanner, FunctionFactory, RelationPlanner,
and table functions) are treated as planning-critical components that must
be installed before any plan-bundle construction.

The platform installation pattern:

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.udf.platform import (
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

import importlib
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast
from weakref import WeakSet

from datafusion import SessionContext

from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.expr.planner import (
    expr_planner_payloads,
    install_expr_planners,
)
from datafusion_engine.udf.catalog import rewrite_tag_index
from datafusion_engine.udf.factory import (
    FunctionFactoryPolicy,
    function_factory_payloads,
    function_factory_policy_from_snapshot,
    install_function_factory,
)
from datafusion_engine.udf.runtime import (
    register_rust_udfs,
    rust_udf_docs,
    rust_udf_snapshot_hash,
)
from utils.env_utils import env_bool


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
_LOGGER = logging.getLogger(__name__)
_SOFT_FAIL_LOGGED: dict[str, bool] = {"expr_planners": False, "function_factory": False}


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


def _resolve_udf_snapshot(
    ctx: SessionContext,
    resolved: RustUdfPlatformOptions,
) -> tuple[
    Mapping[str, object] | None,
    str | None,
    tuple[str, ...],
    Mapping[str, object] | None,
]:
    snapshot: Mapping[str, object] | None = None
    docs: Mapping[str, object] | None = None
    snapshot_hash: str | None = None
    rewrite_tags: tuple[str, ...] = ()
    if not resolved.enable_udfs:
        return snapshot, snapshot_hash, rewrite_tags, docs
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
    return snapshot, snapshot_hash, rewrite_tags, docs


def _resolve_expr_planner_names(
    resolved: RustUdfPlatformOptions,
    snapshot: Mapping[str, object] | None,
) -> tuple[str, ...]:
    planner_names = tuple(resolved.expr_planner_names)
    if resolved.enable_expr_planners and resolved.expr_planner_hook is None:
        derived_planners = domain_planner_names_from_snapshot(snapshot)
        if planner_names:
            return tuple(dict.fromkeys((*planner_names, *derived_planners)))
        return derived_planners
    return planner_names


def _resolve_function_factory_policy(
    resolved: RustUdfPlatformOptions,
    snapshot: Mapping[str, object] | None,
) -> FunctionFactoryPolicy | None:
    if resolved.function_factory_policy is not None or snapshot is None:
        return resolved.function_factory_policy
    return function_factory_policy_from_snapshot(
        snapshot,
        allow_async=resolved.enable_async_udfs,
    )


def _strict_failure_message(
    status_label: str,
    status: ExtensionInstallStatus | None,
) -> str | None:
    if status is None or status.error is None:
        return None
    return f"{status_label} installation failed; native extension is required. {status.error}"


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
    snapshot, snapshot_hash, rewrite_tags, docs = _resolve_udf_snapshot(ctx, resolved)
    planner_names = _resolve_expr_planner_names(resolved, snapshot)
    function_factory_policy = _resolve_function_factory_policy(resolved, snapshot)
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
        allow_soft_fail = env_bool("CODEANATOMY_DIAGNOSTICS_BUNDLE", default=False)
        strict_checks = (
            ("FunctionFactory", function_factory, "function_factory"),
            ("ExprPlanner", expr_planners, "expr_planners"),
        )
        for label, status, log_key in strict_checks:
            msg = _strict_failure_message(label, status)
            if msg is None:
                continue
            if allow_soft_fail:
                if not _SOFT_FAIL_LOGGED[log_key]:
                    _LOGGER.error(msg)
                    _SOFT_FAIL_LOGGED[log_key] = True
            else:
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


def native_udf_platform_available() -> bool:
    """Return whether native FunctionFactory/ExprPlanner hooks are available.

    Returns
    -------
    bool
        ``True`` when the native UDF platform hooks are available.
    """
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if callable(getattr(module, "install_function_factory", None)) and callable(
            getattr(module, "install_expr_planners", None)
        ):
            return True
    return False


def ensure_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
) -> Mapping[str, object]:
    """Ensure Rust UDFs are registered via the unified platform.

    Returns
    -------
    Mapping[str, object]
        Rust UDF registry snapshot.

    Raises
    ------
    RuntimeError
        Raised when the Rust UDF registry snapshot is unavailable.
    """
    platform = install_rust_udf_platform(
        ctx,
        options=RustUdfPlatformOptions(
            enable_udfs=True,
            enable_async_udfs=enable_async,
            async_udf_timeout_ms=async_udf_timeout_ms,
            async_udf_batch_size=async_udf_batch_size,
            enable_function_factory=False,
            enable_expr_planners=False,
            strict=False,
        ),
    )
    if platform.snapshot is None:
        msg = "Rust UDF platform did not return a registry snapshot."
        raise RuntimeError(msg)
    return platform.snapshot


__all__ = [
    "ExtensionInstallStatus",
    "RustUdfPlatform",
    "RustUdfPlatformOptions",
    "ensure_rust_udfs",
    "install_rust_udf_platform",
    "native_udf_platform_available",
]
