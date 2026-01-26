"""Rust UDF registration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from weakref import WeakKeyDictionary, WeakSet

from datafusion import SessionContext

import datafusion_ext

_RUST_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_RUST_UDF_SNAPSHOTS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = (
    WeakKeyDictionary()
)
_RUST_UDF_DOCS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_POLICIES: WeakKeyDictionary[
    SessionContext,
    tuple[bool, int | None, int | None],
] = WeakKeyDictionary()


def _build_registry_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    snapshot = datafusion_ext.registry_snapshot(ctx)
    if not isinstance(snapshot, Mapping):
        msg = "datafusion_ext.registry_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    payload = dict(snapshot)
    payload.setdefault("scalar", [])
    payload.setdefault("aggregate", [])
    payload.setdefault("window", [])
    payload.setdefault("table", [])
    payload.setdefault("aliases", {})
    payload.setdefault("pycapsule_udfs", [])
    payload.setdefault("parameter_names", {})
    payload.setdefault("volatility", {})
    payload.setdefault("rewrite_tags", {})
    payload.setdefault("signature_inputs", {})
    payload.setdefault("return_types", {})
    payload.setdefault("custom_udfs", [])
    return payload


def _notify_ibis_snapshot(snapshot: Mapping[str, object]) -> None:
    try:
        from ibis_engine.builtin_udfs import register_ibis_udf_snapshot
    except ImportError:
        return
    register_ibis_udf_snapshot(snapshot)


def _build_docs_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    snapshot = datafusion_ext.udf_docs_snapshot(ctx)
    if not isinstance(snapshot, Mapping):
        msg = "datafusion_ext.udf_docs_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    return dict(snapshot)


def rust_udf_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    """Return cached Rust UDF registry snapshot for a session.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    Mapping[str, object]
        Registry snapshot payload for diagnostics.
    """
    cached = _RUST_UDF_SNAPSHOTS.get(ctx)
    if cached is not None:
        return cached
    snapshot = _build_registry_snapshot(ctx)
    _notify_ibis_snapshot(snapshot)
    _RUST_UDF_SNAPSHOTS[ctx] = snapshot
    return snapshot


def rust_udf_docs(ctx: SessionContext) -> Mapping[str, object]:
    """Return cached Rust UDF documentation snapshot for a session.

    Parameters
    ----------
    ctx
        DataFusion session context.

    Returns
    -------
    Mapping[str, object]
        Documentation snapshot payload for diagnostics.
    """
    cached = _RUST_UDF_DOCS.get(ctx)
    if cached is not None:
        return cached
    snapshot = _build_docs_snapshot(ctx)
    _RUST_UDF_DOCS[ctx] = snapshot
    return snapshot


def register_rust_udfs(
    ctx: SessionContext,
    *,
    enable_async: bool = False,
    async_udf_timeout_ms: int | None = None,
    async_udf_batch_size: int | None = None,
) -> Mapping[str, object]:
    """Register Rust-backed UDFs once per session context.

    Returns
    -------
    Mapping[str, object]
        Snapshot payload for diagnostics.
    """
    if not enable_async and (async_udf_timeout_ms is not None or async_udf_batch_size is not None):
        msg = "Async UDF policy provided but enable_async is False."
        raise ValueError(msg)
    if enable_async:
        if async_udf_timeout_ms is None or async_udf_timeout_ms <= 0:
            msg = "async_udf_timeout_ms must be a positive integer when async UDFs are enabled."
            raise ValueError(msg)
        if async_udf_batch_size is None or async_udf_batch_size <= 0:
            msg = "async_udf_batch_size must be a positive integer when async UDFs are enabled."
            raise ValueError(msg)
    policy = (enable_async, async_udf_timeout_ms, async_udf_batch_size)
    if ctx in _RUST_UDF_CONTEXTS:
        existing = _RUST_UDF_POLICIES.get(ctx)
        if (
            existing is not None
            and existing != policy
            and not (
                not enable_async
                and async_udf_timeout_ms is None
                and async_udf_batch_size is None
            )
        ):
            msg = "Rust UDFs already registered with a different async policy."
            raise ValueError(msg)
        return rust_udf_snapshot(ctx)
    datafusion_ext.register_udfs(
        ctx,
        enable_async,
        async_udf_timeout_ms,
        async_udf_batch_size,
    )
    _RUST_UDF_CONTEXTS.add(ctx)
    _RUST_UDF_POLICIES[ctx] = policy
    return rust_udf_snapshot(ctx)


__all__ = ["register_rust_udfs", "rust_udf_docs", "rust_udf_snapshot"]
