"""Rust UDF registration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from weakref import WeakSet

from datafusion import SessionContext

import datafusion_ext

_RUST_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()

def _build_registry_snapshot() -> Mapping[str, object]:
    snapshot = datafusion_ext.udf_registry_snapshot()
    if not isinstance(snapshot, Mapping):
        msg = "datafusion_ext.udf_registry_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    payload = dict(snapshot)
    payload.setdefault("scalar", [])
    payload.setdefault("aggregate", [])
    payload.setdefault("window", [])
    payload.setdefault("table", [])
    payload.setdefault("aliases", {})
    payload.setdefault("pycapsule_udfs", [])
    return payload


def _build_docs_snapshot() -> Mapping[str, object]:
    snapshot = datafusion_ext.udf_docs_snapshot()
    if not isinstance(snapshot, Mapping):
        msg = "datafusion_ext.udf_docs_snapshot returned a non-mapping payload."
        raise TypeError(msg)
    return dict(snapshot)


RUST_UDF_SNAPSHOT: Mapping[str, object] = _build_registry_snapshot()
RUST_UDF_DOCS: Mapping[str, object] = _build_docs_snapshot()


def register_rust_udfs(ctx: SessionContext) -> Mapping[str, object]:
    """Register Rust-backed UDFs once per session context.

    Returns
    -------
    Mapping[str, object]
        Snapshot payload for diagnostics.
    """
    if ctx in _RUST_UDF_CONTEXTS:
        return RUST_UDF_SNAPSHOT
    datafusion_ext.register_udfs(ctx)
    _RUST_UDF_CONTEXTS.add(ctx)
    return RUST_UDF_SNAPSHOT


__all__ = ["RUST_UDF_DOCS", "RUST_UDF_SNAPSHOT", "register_rust_udfs"]
