"""SchemaRuntime bridge helpers shared across schema-policy call paths."""

from __future__ import annotations

import importlib
from functools import lru_cache
from typing import Protocol, cast


class SchemaRuntime(Protocol):
    """Runtime protocol for Rust schema-policy bridge methods."""

    def apply_scan_policy_json(self, scan_policy_json: str, defaults_json: str) -> str:
        """Apply general scan-policy defaults and return merged JSON payload."""
        ...

    def apply_delta_scan_policy_json(self, delta_scan_json: str, defaults_json: str) -> str:
        """Apply Delta scan-policy defaults and return merged JSON payload."""
        ...


@lru_cache(maxsize=1)
def load_schema_runtime() -> SchemaRuntime:
    """Return a cached ``codeanatomy_engine.SchemaRuntime`` bridge instance.

    Raises:
        RuntimeError: If the extension module or required class is unavailable.
    """
    try:
        module = importlib.import_module("codeanatomy_engine")
    except ImportError as exc:
        msg = (
            "codeanatomy_engine.SchemaRuntime is required for scan-policy "
            "resolution; install/build the Rust extension for this environment."
        )
        raise RuntimeError(msg) from exc
    runtime_cls = getattr(module, "SchemaRuntime", None)
    if runtime_cls is None:
        msg = (
            "codeanatomy_engine.SchemaRuntime is required for scan-policy "
            "resolution, but the class is not exported by the extension."
        )
        raise RuntimeError(msg)
    try:
        return cast("SchemaRuntime", runtime_cls())
    except (TypeError, ValueError) as exc:
        msg = "Failed to initialize codeanatomy_engine.SchemaRuntime."
        raise RuntimeError(msg) from exc


__all__ = ["SchemaRuntime", "load_schema_runtime"]
