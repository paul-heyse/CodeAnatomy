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
    return payload


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


def register_rust_udfs(ctx: SessionContext) -> Mapping[str, object]:
    """Register Rust-backed UDFs once per session context.

    Returns
    -------
    Mapping[str, object]
        Snapshot payload for diagnostics.
    """
    if ctx in _RUST_UDF_CONTEXTS:
        return rust_udf_snapshot(ctx)
    datafusion_ext.register_udfs(ctx)
    _RUST_UDF_CONTEXTS.add(ctx)
    return rust_udf_snapshot(ctx)


__all__ = ["register_rust_udfs", "rust_udf_docs", "rust_udf_snapshot"]
