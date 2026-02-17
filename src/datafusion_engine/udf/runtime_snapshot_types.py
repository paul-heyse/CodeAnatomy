"""Canonical runtime install snapshot contracts for Rust UDF runtime."""

from __future__ import annotations

from collections.abc import Mapping

from serde_msgspec import StructBaseStrict


class RuntimeInstallSnapshot(StructBaseStrict, frozen=True):
    """Normalized runtime install payload returned by native extension."""

    contract_version: int = 3
    runtime_install_mode: str = "unified"
    udf_installed: bool = True
    function_factory_installed: bool = True
    expr_planners_installed: bool = True
    cache_registrar_available: bool = False
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
        snapshot=_coerce_snapshot(payload.get("snapshot")),
    )


__all__ = ["RuntimeInstallSnapshot", "normalize_runtime_install_snapshot"]
