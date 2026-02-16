# ruff: noqa: D103, INP001
"""Tests for Delta write split module."""

from __future__ import annotations

import inspect

from storage.deltalake import delta_write


def test_delta_write_module_exposes_commit_builders() -> None:
    assert callable(delta_write.build_commit_properties)
    assert callable(delta_write.idempotent_commit_properties)


def test_delta_write_module_contains_local_commit_logic() -> None:
    source = inspect.getsource(delta_write)
    assert "def build_commit_properties" in source
    assert "delta_read as _core" not in source
