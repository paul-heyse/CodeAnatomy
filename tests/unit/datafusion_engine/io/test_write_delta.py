# ruff: noqa: D103, INP001
"""Tests for write-delta split module."""

from __future__ import annotations

import inspect

from datafusion_engine.io import write_delta


def test_write_delta_exports_core_types() -> None:
    assert hasattr(write_delta, "WritePipeline")
    assert hasattr(write_delta, "DeltaWriteSpec")


def test_write_delta_owns_delta_policy_helpers() -> None:
    source = inspect.getsource(write_delta)
    assert "def _resolve_delta_schema_policy" in source
    assert "write_core as _core" not in source
