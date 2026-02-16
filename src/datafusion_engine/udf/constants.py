"""Shared constants for Rust UDF extension integration."""

from __future__ import annotations

EXTENSION_MODULE_PATH = "datafusion_engine.extensions.datafusion_ext"
EXTENSION_MODULE_LABEL = "datafusion_ext"
EXTENSION_MODULE_PREFIX = "datafusion_engine.extensions."
EXTENSION_ENTRY_POINT = "register"
REBUILD_WHEELS_HINT = (
    "Rebuild and install matching datafusion/datafusion_ext wheels "
    "(scripts/build_datafusion_wheels.sh + uv sync)."
)

ABI_MISMATCH_PREFIX = "Extension ABI mismatch: "
ABI_VERSION_MISMATCH_MSG = (
    "Extension ABI version mismatch: expected {expected}, got {actual}. "
    "Rebuild the extension against the current runtime."
)
ABI_LOAD_FAILURE_MSG = "Failed to load extension module {module!r}: {error}"

__all__ = [
    "ABI_LOAD_FAILURE_MSG",
    "ABI_MISMATCH_PREFIX",
    "ABI_VERSION_MISMATCH_MSG",
    "EXTENSION_ENTRY_POINT",
    "EXTENSION_MODULE_LABEL",
    "EXTENSION_MODULE_PATH",
    "EXTENSION_MODULE_PREFIX",
    "REBUILD_WHEELS_HINT",
]
