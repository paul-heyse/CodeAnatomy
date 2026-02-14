"""Contracts for Rust UDF platform installation."""

from __future__ import annotations

import msgspec


class InstallRustUdfPlatformRequestV1(msgspec.Struct, frozen=True):
    """Serializable request envelope for Rust UDF platform installation."""

    options: dict[str, object] | None = None


__all__ = ["InstallRustUdfPlatformRequestV1"]
