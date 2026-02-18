"""Canonical runtime install snapshot contracts for Rust UDF runtime."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from serde_msgspec import (
    StructBaseCompat,
    StructBaseStrict,
    convert,
    loads_msgpack,
    to_builtins,
)

type ConfigDefaultValue = bool | int | str


class RustUdfSnapshot(StructBaseCompat, frozen=True):
    """Typed UDF snapshot contract emitted by Rust runtime payloads."""

    version: int = 1
    scalar: tuple[str, ...] = ()
    aggregate: tuple[str, ...] = ()
    window: tuple[str, ...] = ()
    table: tuple[str, ...] = ()
    aliases: dict[str, tuple[str, ...]] = msgspec.field(default_factory=dict)
    parameter_names: dict[str, tuple[str, ...]] = msgspec.field(default_factory=dict)
    volatility: dict[str, str] = msgspec.field(default_factory=dict)
    rewrite_tags: dict[str, tuple[str, ...]] = msgspec.field(default_factory=dict)
    simplify: dict[str, bool] = msgspec.field(default_factory=dict)
    coerce_types: dict[str, bool] = msgspec.field(default_factory=dict)
    short_circuits: dict[str, bool] = msgspec.field(default_factory=dict)
    signature_inputs: dict[str, tuple[tuple[str, ...], ...]] = msgspec.field(default_factory=dict)
    return_types: dict[str, tuple[str, ...]] = msgspec.field(default_factory=dict)
    config_defaults: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)
    custom_udfs: tuple[str, ...] = ()
    pycapsule_udfs: tuple[str, ...] = ()


class RuntimeInstallSnapshot(StructBaseStrict, frozen=True):
    """Normalized runtime install payload returned by native extension."""

    contract_version: int = 3
    runtime_install_mode: str = "unified"
    udf_installed: bool = True
    function_factory_installed: bool = True
    expr_planners_installed: bool = True
    cache_registrar_available: bool = False
    snapshot_msgpack: bytes | None = None
    snapshot: Mapping[str, object] = {}


def _coerce_int(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _coerce_snapshot(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): inner for key, inner in value.items()}


def _coerce_optional_bytes(value: object) -> bytes | None:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return None


def decode_rust_udf_snapshot_msgpack(payload: bytes) -> RustUdfSnapshot:
    """Decode a Rust UDF snapshot msgpack payload into its typed contract.

    Returns:
    -------
    RustUdfSnapshot
        Typed runtime snapshot contract.
    """
    return loads_msgpack(payload, target_type=RustUdfSnapshot, strict=False)


def coerce_rust_udf_snapshot(snapshot: Mapping[str, object]) -> RustUdfSnapshot:
    """Coerce a mapping snapshot into the typed Rust UDF snapshot contract.

    Returns:
    -------
    RustUdfSnapshot
        Typed runtime snapshot contract.
    """
    return convert(snapshot, target_type=RustUdfSnapshot, strict=False)


def rust_udf_snapshot_mapping(snapshot: RustUdfSnapshot) -> dict[str, object]:
    """Convert a typed Rust UDF snapshot into a deterministic mapping payload.

    Returns:
    -------
    dict[str, object]
        Deterministic mapping payload for runtime artifact recording.
    """
    builtins = to_builtins(snapshot, str_keys=True)
    if not isinstance(builtins, Mapping):
        return {}
    return {str(key): value for key, value in builtins.items()}


def normalize_runtime_install_snapshot(payload: Mapping[str, object]) -> RuntimeInstallSnapshot:
    """Normalize arbitrary mapping payload into canonical runtime snapshot contract.

    Returns:
        RuntimeInstallSnapshot: Canonical runtime snapshot contract.
    """
    return RuntimeInstallSnapshot(
        contract_version=_coerce_int(payload.get("contract_version", 3), default=3),
        runtime_install_mode=str(payload.get("runtime_install_mode", "unified")),
        udf_installed=bool(payload.get("udf_installed", True)),
        function_factory_installed=bool(payload.get("function_factory_installed", True)),
        expr_planners_installed=bool(payload.get("expr_planners_installed", True)),
        cache_registrar_available=bool(payload.get("cache_registrar_available", False)),
        snapshot_msgpack=_coerce_optional_bytes(payload.get("snapshot_msgpack")),
        snapshot=_coerce_snapshot(payload.get("snapshot")),
    )


__all__ = [
    "ConfigDefaultValue",
    "RuntimeInstallSnapshot",
    "RustUdfSnapshot",
    "coerce_rust_udf_snapshot",
    "decode_rust_udf_snapshot_msgpack",
    "normalize_runtime_install_snapshot",
    "rust_udf_snapshot_mapping",
]
