"""Validation helpers for Rust UDF extension capabilities."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion import SessionContext

from datafusion_engine.udf.extension_runtime import (
    extension_capabilities_report,
    validate_extension_capabilities,
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
    return validate_extension_capabilities(strict=strict, ctx=ctx)


def capability_report() -> Mapping[str, object]:
    """Return extension capability diagnostics report."""
    return extension_capabilities_report()


__all__ = ["capability_report", "validate_runtime_capabilities"]
