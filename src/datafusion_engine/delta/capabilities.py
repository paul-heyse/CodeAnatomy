"""Delta extension capability checks."""

from __future__ import annotations

import importlib
from dataclasses import dataclass

from datafusion import SessionContext


@dataclass(frozen=True)
class DeltaExtensionCompatibility:
    """Compatibility status for the Delta extension module."""

    available: bool
    compatible: bool
    error: str | None


def is_delta_extension_compatible(
    ctx: SessionContext,
    *,
    entrypoint: str = "delta_scan_config_from_session",
) -> DeltaExtensionCompatibility:
    """Return whether the Delta extension entrypoint can be invoked safely.

    Returns
    -------
    DeltaExtensionCompatibility
        Availability/compatibility status plus error details if any.
    """
    module = _resolve_extension_module(entrypoint=entrypoint)
    if module is None:
        return DeltaExtensionCompatibility(
            available=False,
            compatible=False,
            error="Delta extension module is unavailable.",
        )
    probe = getattr(module, entrypoint, None)
    if not callable(probe):
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error=f"Delta entrypoint {entrypoint} is unavailable.",
        )
    internal_ctx = getattr(ctx, "ctx", None)
    if internal_ctx is None:
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error="Delta entrypoints require SessionContext.ctx.",
        )
    try:
        probe(internal_ctx, None, None, None, None, None)
    except (TypeError, RuntimeError, ValueError) as exc:
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error=str(exc),
        )
    return DeltaExtensionCompatibility(available=True, compatible=True, error=None)


def _resolve_extension_module(entrypoint: str | None = None) -> object | None:
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if entrypoint is not None and not callable(getattr(module, entrypoint, None)):
            continue
        return module
    return None


__all__ = ["DeltaExtensionCompatibility", "is_delta_extension_compatible"]
