"""Validation helpers for Rust UDF extension capabilities."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.udf.extension_core import (
    ABI_VERSION_MISMATCH_MSG,
    REBUILD_WHEELS_HINT,
    _EXPECTED_PLUGIN_ABI_MAJOR,
    _EXPECTED_PLUGIN_ABI_MINOR,
    _extension_module_with_capabilities,
    _invoke_runtime_entrypoint,
)


def validate_runtime_capabilities(
    *,
    strict: bool = True,
    ctx: SessionContext | None = None,
) -> Mapping[str, object]:
    """Validate extension capabilities for the runtime profile.

    Returns:
    -------
    Mapping[str, object]
        Capability report payload.
    """
    report = capability_report()
    if strict and not report.get("compatible", False):
        msg = report.get("error") or "Extension ABI compatibility check failed."
        raise RuntimeError(msg)
    module = _extension_module_with_capabilities()
    probe = getattr(module, "session_context_contract_probe", None) if module is not None else None
    if strict and module is not None and callable(probe):
        probe_ctx = ctx if ctx is not None else SessionContext()
        try:
            _invoke_runtime_entrypoint(module, "session_context_contract_probe", ctx=probe_ctx)
        except (RuntimeError, TypeError, ValueError) as exc:
            expected = {"major": _EXPECTED_PLUGIN_ABI_MAJOR, "minor": _EXPECTED_PLUGIN_ABI_MINOR}
            msg = (
                "SessionContext ABI mismatch detected by extension contract probe. "
                f"expected_plugin_abi={expected}. "
                f"{REBUILD_WHEELS_HINT}"
            )
            raise RuntimeError(msg) from exc
    return report


def capability_report() -> Mapping[str, object]:
    """Return extension capability diagnostics report."""
    expected = {"major": _EXPECTED_PLUGIN_ABI_MAJOR, "minor": _EXPECTED_PLUGIN_ABI_MINOR}
    try:
        snapshot = extension_capabilities_snapshot()
    except (ImportError, TypeError, RuntimeError, ValueError) as exc:
        return {
            "available": False,
            "compatible": False,
            "expected_plugin_abi": expected,
            "error": str(exc),
            "snapshot": None,
        }
    plugin_abi = snapshot.get("plugin_abi") if isinstance(snapshot, Mapping) else None
    major = None
    minor = None
    if isinstance(plugin_abi, Mapping):
        major = plugin_abi.get("major")
        minor = plugin_abi.get("minor")
    compatible = major == _EXPECTED_PLUGIN_ABI_MAJOR and minor == _EXPECTED_PLUGIN_ABI_MINOR
    error = None
    if not compatible:
        error = ABI_VERSION_MISMATCH_MSG.format(
            expected=expected,
            actual={"major": major, "minor": minor},
        )
    return {
        "available": True,
        "compatible": compatible,
        "expected_plugin_abi": expected,
        "observed_plugin_abi": {"major": major, "minor": minor},
        "snapshot": snapshot,
        "error": error,
    }


def extension_capabilities_snapshot() -> Mapping[str, object]:
    """Return the Rust extension capabilities snapshot when available."""
    from datafusion_engine.udf.extension_core import (
        EXTENSION_MODULE_LABEL,
    )

    module = _extension_module_with_capabilities()
    snapshot = module.capabilities_snapshot()
    if isinstance(snapshot, Mapping):
        return dict(snapshot)
    msg = f"{EXTENSION_MODULE_LABEL}.capabilities_snapshot returned a non-mapping payload."
    raise TypeError(msg)

__all__ = [
    "capability_report",
    "extension_capabilities_snapshot",
    "validate_runtime_capabilities",
]
