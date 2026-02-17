"""Tests for write-delta split module."""

from __future__ import annotations

import inspect

from datafusion_engine.io import write_delta


def test_write_delta_exports_core_types() -> None:
    """write_delta module exports core pipeline type aliases."""
    assert hasattr(write_delta, "WritePipeline")
    assert hasattr(write_delta, "DeltaWriteSpec")


def test_write_delta_owns_delta_policy_helpers() -> None:
    """write_delta module owns local delta policy helper logic."""
    source = inspect.getsource(write_delta)
    assert "def _resolve_delta_schema_policy" in source
    assert "write_core as _core" not in source
