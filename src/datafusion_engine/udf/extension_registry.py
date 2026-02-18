"""Registry-facing UDF extension helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.udf.extension_runtime import (
    ExtensionRegistries,
    _async_udf_policy,
    _build_docs_snapshot,
    _build_registry_snapshot,
    _install_rust_udfs,
    _notify_udf_snapshot,
    _registered_snapshot,
    _resolve_registries,
    _validated_snapshot,
)
from datafusion_engine.udf.extension_validation import (
    _iter_snapshot_values,
    _snapshot_alias_names,
    validate_rust_udf_snapshot,
)


def register_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Register Rust UDFs into a session context.

    Returns:
        Mapping[str, object]: Validated UDF snapshot for the context.

    Raises:
        ValueError: If async UDF policy settings are invalid.
    """
    try:
        policy = _async_udf_policy(
            enable_async=enable_async,
            async_udf_timeout_ms=async_udf_timeout_ms,
            async_udf_batch_size=async_udf_batch_size,
        )
    except ValueError as exc:
        msg = f"Invalid async UDF policy: {exc}"
        raise ValueError(msg) from exc
    resolved_registries = _resolve_registries(registries)
    existing = _registered_snapshot(
        ctx,
        policy=policy,
        registries=resolved_registries,
    )
    if existing is not None:
        return existing
    _install_rust_udfs(
        ctx,
        enable_async=enable_async,
        async_udf_timeout_ms=async_udf_timeout_ms,
        async_udf_batch_size=async_udf_batch_size,
        registries=resolved_registries,
    )
    resolved_registries.udf_contexts.add(ctx)
    resolved_registries.udf_policies[ctx] = policy
    return _validated_snapshot(ctx, registries=resolved_registries)


def rust_udf_snapshot(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Capture a runtime snapshot of registered Rust UDFs.

    Returns:
        Mapping[str, object]: Registered UDF snapshot for the context.
    """
    resolved_registries = _resolve_registries(registries)
    cached = resolved_registries.udf_snapshots.get(ctx)
    if cached is not None:
        if ctx not in resolved_registries.udf_validated:
            validate_rust_udf_snapshot(cached)
            resolved_registries.udf_validated.add(ctx)
        return cached
    snapshot = _build_registry_snapshot(ctx, registries=resolved_registries)
    docs = _build_docs_snapshot(ctx)
    if docs:
        snapshot = dict(snapshot)
        snapshot["documentation"] = docs
    validate_rust_udf_snapshot(snapshot)
    resolved_registries.udf_validated.add(ctx)
    _notify_udf_snapshot(snapshot)
    resolved_registries.udf_snapshots[ctx] = snapshot
    return snapshot


def snapshot_function_names(
    snapshot: Mapping[str, object],
    *,
    include_aliases: bool = False,
    include_custom: bool = False,
) -> frozenset[str]:
    """Snapshot sorted runtime function names.

    Returns:
        frozenset[str]: Discovered function names from selected snapshot sections.
    """
    keys: tuple[str, ...] = ("scalar", "aggregate", "window", "table")
    if include_custom:
        keys = (*keys, "custom_udfs")
    names: set[str] = set()
    for key in keys:
        names.update(_iter_snapshot_values(snapshot.get(key)))
    if include_aliases:
        names.update(_snapshot_alias_names(snapshot))
    return frozenset(names)


__all__ = [
    "ExtensionRegistries",
    "register_rust_udfs",
    "rust_udf_snapshot",
    "snapshot_function_names",
]
