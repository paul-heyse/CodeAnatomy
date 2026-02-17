"""Tests for storage.deltalake split modules."""

from __future__ import annotations

import inspect

from storage.deltalake import delta_maintenance, delta_metadata


def test_delta_maintenance_contains_maintenance_entrypoints() -> None:
    """delta_maintenance module owns maintenance entrypoint functions."""
    source = inspect.getsource(delta_maintenance)
    assert "def vacuum_delta" in source
    assert "def cleanup_delta_log" in source
    assert "delta_read as _core" not in source


def test_delta_metadata_contains_metadata_entrypoints() -> None:
    """delta_metadata module owns metadata entrypoint functions."""
    source = inspect.getsource(delta_metadata)
    assert "def canonical_table_uri" in source
    assert "def delta_table_schema" in source
    assert "delta_read as _core" not in source
