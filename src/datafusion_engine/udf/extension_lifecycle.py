"""Lifecycle helpers for Rust UDF registration."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.udf.extension_runtime import (
    ExtensionRegistries,
    register_rust_udfs,
    register_udfs_via_ddl,
)


def install_udfs(
    ctx: SessionContext,
    *,
    registries: ExtensionRegistries | None = None,
) -> Mapping[str, object]:
    """Install Rust UDFs and return the runtime snapshot.

    Returns:
    -------
    Mapping[str, object]
        Runtime UDF snapshot payload.
    """
    return register_rust_udfs(ctx, registries=registries)


def install_udf_ddl(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    registries: ExtensionRegistries | None = None,
) -> None:
    """Install UDF DDL definitions for catalog parity."""
    register_udfs_via_ddl(ctx, snapshot=snapshot, registries=registries)


__all__ = ["install_udf_ddl", "install_udfs"]
