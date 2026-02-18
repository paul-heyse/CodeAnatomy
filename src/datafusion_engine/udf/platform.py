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

    profile = existing_profile
    ctx = profile.session_context()
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

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast
from weakref import WeakSet

import msgspec
from datafusion import SessionContext

from datafusion_engine.expr.domain_planner import domain_planner_names_from_snapshot
from datafusion_engine.expr.planner import (
    expr_planner_payloads,
    install_expr_planners,
)
from datafusion_engine.udf.contracts import InstallRustUdfPlatformRequestV1
from datafusion_engine.udf.extension_runtime import (
    ExtensionRegistries,
    register_rust_udfs,
    rust_runtime_install_payload,
    rust_udf_docs,
    rust_udf_snapshot_hash,
)
from datafusion_engine.udf.factory import (
    FunctionFactoryPolicy,
    function_factory_payloads,
    function_factory_policy_from_snapshot,
    install_function_factory,
)
from datafusion_engine.udf.metadata import rewrite_tag_index


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


@dataclass
class RustUdfPlatformRegistries:
    """Injectable runtime registries for platform installation state."""

    function_factory_ctxs: WeakSet[SessionContext] = field(default_factory=WeakSet)
    expr_planner_ctxs: WeakSet[SessionContext] = field(default_factory=WeakSet)


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PlatformInstallState:
    snapshot: Mapping[str, object] | None
    snapshot_hash: str | None
    rewrite_tags: tuple[str, ...]
    docs: Mapping[str, object] | None
    runtime_payload: Mapping[str, object]
    planner_names: tuple[str, ...]
    function_factory_policy: FunctionFactoryPolicy | None


def _install_function_factory(
    ctx: SessionContext,
    *,
    enabled: bool,
    hook: Callable[[SessionContext], None] | None,
    policy: FunctionFactoryPolicy | None,
    registries: RustUdfPlatformRegistries,
) -> tuple[ExtensionInstallStatus | None, Mapping[str, object] | None]:
    if not enabled:
        return None, None
    if ctx in registries.function_factory_ctxs:
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
        registries.function_factory_ctxs.add(ctx)
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
    registries: RustUdfPlatformRegistries,
) -> tuple[ExtensionInstallStatus | None, Mapping[str, object] | None]:
    if not enabled:
        return None, None
    if ctx in registries.expr_planner_ctxs:
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
        registries.expr_planner_ctxs.add(ctx)
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
    *,
    extension_registries: ExtensionRegistries | None = None,
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
        registries=extension_registries,
    )
    snapshot_hash = rust_udf_snapshot_hash(snapshot)
    tag_index = rewrite_tag_index(snapshot)
    rewrite_tags = tuple(sorted(tag_index))
    docs_value = snapshot.get("documentation") if isinstance(snapshot, Mapping) else None
    docs = cast("Mapping[str, object] | None", docs_value)
    if docs is None:
        docs = rust_udf_docs(ctx, registries=extension_registries)
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
    if resolved.enable_expr_planners and resolved.expr_planner_hook is None and not planner_names:
        return ("codeanatomy_domain",)
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


def _resolve_platform_install_state(
    *,
    ctx: SessionContext,
    resolved: RustUdfPlatformOptions,
    extension_registries: ExtensionRegistries | None = None,
) -> _PlatformInstallState:
    snapshot, snapshot_hash, rewrite_tags, docs = _resolve_udf_snapshot(
        ctx,
        resolved,
        extension_registries=extension_registries,
    )
    runtime_payload = (
        rust_runtime_install_payload(ctx, registries=extension_registries)
        if snapshot is not None
        else cast("Mapping[str, object]", {})
    )
    return _PlatformInstallState(
        snapshot=snapshot,
        snapshot_hash=snapshot_hash,
        rewrite_tags=rewrite_tags,
        docs=docs,
        runtime_payload=runtime_payload,
        planner_names=_resolve_expr_planner_names(resolved, snapshot),
        function_factory_policy=_resolve_function_factory_policy(resolved, snapshot),
    )


def _install_function_factory_status(
    *,
    ctx: SessionContext,
    resolved: RustUdfPlatformOptions,
    state: _PlatformInstallState,
    registries: RustUdfPlatformRegistries,
) -> tuple[ExtensionInstallStatus | None, Mapping[str, object] | None]:
    function_factory_payload = (
        function_factory_payloads(state.function_factory_policy)
        if resolved.enable_function_factory
        else None
    )
    if not resolved.enable_function_factory:
        return None, function_factory_payload
    if resolved.function_factory_hook is not None:
        return _install_function_factory(
            ctx,
            enabled=True,
            hook=resolved.function_factory_hook,
            policy=state.function_factory_policy,
            registries=registries,
        )
    if state.snapshot is not None and "function_factory_installed" in state.runtime_payload:
        installed = bool(state.runtime_payload.get("function_factory_installed"))
        error = (
            None if installed else "install_codeanatomy_runtime did not install FunctionFactory."
        )
        return ExtensionInstallStatus(
            available=True, installed=installed, error=error
        ), function_factory_payload
    return _install_function_factory(
        ctx,
        enabled=True,
        hook=None,
        policy=state.function_factory_policy,
        registries=registries,
    )


def _install_expr_planner_status(
    *,
    ctx: SessionContext,
    resolved: RustUdfPlatformOptions,
    state: _PlatformInstallState,
    registries: RustUdfPlatformRegistries,
) -> tuple[ExtensionInstallStatus | None, Mapping[str, object] | None]:
    expr_planner_payload = (
        expr_planner_payloads(state.planner_names) if resolved.enable_expr_planners else None
    )
    if not resolved.enable_expr_planners:
        return None, expr_planner_payload
    if resolved.expr_planner_hook is not None:
        return _install_expr_planners(
            ctx,
            enabled=True,
            hook=resolved.expr_planner_hook,
            planner_names=state.planner_names,
            registries=registries,
        )
    if state.snapshot is not None and "expr_planners_installed" in state.runtime_payload:
        installed = bool(state.runtime_payload.get("expr_planners_installed"))
        error = None if installed else "install_codeanatomy_runtime did not install ExprPlanners."
        return ExtensionInstallStatus(
            available=True, installed=installed, error=error
        ), expr_planner_payload
    return _install_expr_planners(
        ctx,
        enabled=True,
        hook=None,
        planner_names=state.planner_names,
        registries=registries,
    )


def install_rust_udf_platform(
    request: InstallRustUdfPlatformRequestV1,
    *,
    ctx: SessionContext,
    registries: RustUdfPlatformRegistries | None = None,
    extension_registries: ExtensionRegistries | None = None,
) -> RustUdfPlatform:
    """Install planning-critical extension platform before plan-bundle construction.

    Args:
        request: Serializable platform installation request payload.
        ctx: DataFusion session context.
        registries: Optional platform registry container.
        extension_registries: Optional extension-runtime registry container.

    Returns:
        RustUdfPlatform: Result.

    Raises:
        RuntimeError: If strict mode is enabled and required hooks fail.
    """
    resolved = (
        msgspec.convert(request.options, type=RustUdfPlatformOptions)
        if isinstance(request.options, dict)
        else RustUdfPlatformOptions()
    )
    from datafusion_engine.udf.extension_runtime import validate_extension_capabilities

    _ = validate_extension_capabilities(strict=resolved.strict, ctx=ctx)
    resolved_registries = registries or RustUdfPlatformRegistries()
    state = _resolve_platform_install_state(
        ctx=ctx,
        resolved=resolved,
        extension_registries=extension_registries,
    )
    function_factory, function_factory_payload = _install_function_factory_status(
        ctx=ctx,
        resolved=resolved,
        state=state,
        registries=resolved_registries,
    )
    expr_planners, expr_planner_payload = _install_expr_planner_status(
        ctx=ctx,
        resolved=resolved,
        state=state,
        registries=resolved_registries,
    )
    if resolved.strict:
        strict_checks = (
            ("FunctionFactory", function_factory),
            ("ExprPlanner", expr_planners),
        )
        for label, status in strict_checks:
            msg = _strict_failure_message(label, status)
            if msg is not None:
                raise RuntimeError(msg)
    return RustUdfPlatform(
        snapshot=state.snapshot,
        snapshot_hash=state.snapshot_hash,
        rewrite_tags=state.rewrite_tags,
        domain_planner_names=state.planner_names,
        docs=state.docs,
        function_factory=function_factory,
        expr_planners=expr_planners,
        function_factory_policy=function_factory_payload,
        expr_planner_policy=expr_planner_payload,
    )


def ensure_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
    extension_registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Ensure Rust UDFs are registered via the unified platform.

    Args:
        ctx: DataFusion session context.
        enable_async: Whether async UDF execution is enabled.
        async_udf_timeout_ms: Optional async UDF timeout override.
        async_udf_batch_size: Optional async UDF batch-size override.
        extension_registries: Optional extension-runtime registry container.

    Returns:
        Mapping[str, object]: Result.

    Raises:
        RuntimeError: If the platform does not return a registry snapshot.
    """
    platform = install_rust_udf_platform(
        InstallRustUdfPlatformRequestV1(
            options=msgspec.to_builtins(
                RustUdfPlatformOptions(
                    enable_udfs=True,
                    enable_async_udfs=enable_async,
                    async_udf_timeout_ms=async_udf_timeout_ms,
                    async_udf_batch_size=async_udf_batch_size,
                    enable_function_factory=False,
                    enable_expr_planners=False,
                    strict=False,
                )
            )
        ),
        ctx=ctx,
        extension_registries=extension_registries,
    )
    if platform.snapshot is None:
        msg = "Rust UDF platform did not return a registry snapshot."
        raise RuntimeError(msg)
    return platform.snapshot


__all__ = [
    "ExtensionInstallStatus",
    "RustUdfPlatform",
    "RustUdfPlatformOptions",
    "RustUdfPlatformRegistries",
    "ensure_rust_udfs",
    "install_rust_udf_platform",
]
