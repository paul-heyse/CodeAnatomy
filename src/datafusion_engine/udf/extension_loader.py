"""Extension module loading helpers for Rust UDF runtime."""

from __future__ import annotations

import importlib
from types import ModuleType

from datafusion_engine.udf.constants import EXTENSION_MODULE_PATH


def load_extension_module() -> ModuleType:
    """Load the configured Rust UDF extension module.

    Returns:
    -------
    ModuleType
        Loaded extension module object.
    """
    return importlib.import_module(EXTENSION_MODULE_PATH)


__all__ = ["load_extension_module"]
