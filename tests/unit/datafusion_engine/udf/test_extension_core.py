"""Tests for UDF extension split modules."""

from __future__ import annotations

import inspect

from datafusion_engine.udf import extension_registry, extension_runtime


def test_extension_registry_exports_registration_helpers() -> None:
    """Extension registry exports canonical registration helpers."""
    assert callable(extension_registry.register_rust_udfs)
    source = inspect.getsource(extension_registry)
    assert "extension_core" not in source
    assert "extension_snapshot_runtime" not in source
    assert "extension_runtime" in source
    assert "def register_rust_udfs" in source


def test_extension_runtime_exports_runtime_helpers() -> None:
    """Extension runtime module exports runtime helper surface."""
    assert callable(extension_runtime.validate_runtime_capabilities)
