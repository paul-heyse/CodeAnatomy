"""Rust UDF registration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from weakref import WeakSet

from datafusion import SessionContext

import datafusion_ext

_RUST_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()

_SCALAR_UDFS: tuple[str, ...] = (
    "arrow_metadata",
    "col_to_byte",
    "prefixed_hash64",
    "stable_hash64",
    "stable_hash128",
    "stable_id",
)
_TABLE_UDFS: tuple[str, ...] = ("range_table",)

RUST_UDF_SNAPSHOT: Mapping[str, object] = {
    "scalar": list(_SCALAR_UDFS),
    "aggregate": [],
    "window": [],
    "table": list(_TABLE_UDFS),
    "pycapsule_udfs": [],
}


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


__all__ = ["RUST_UDF_SNAPSHOT", "register_rust_udfs"]
