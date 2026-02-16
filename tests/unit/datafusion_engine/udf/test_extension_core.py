# ruff: noqa: D103, INP001
"""Tests for UDF extension split modules."""

from __future__ import annotations

import inspect

from datafusion_engine.udf import extension_registry, extension_validation


def test_extension_registry_exports_registration_helpers() -> None:
    assert callable(extension_registry.register_rust_udfs)
    source = inspect.getsource(extension_registry)
    assert "extension_core as _core" not in source
    assert "def register_rust_udfs" in source


def test_extension_validation_exports_runtime_helpers() -> None:
    assert callable(extension_validation.validate_runtime_capabilities)
