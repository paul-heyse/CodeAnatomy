# ruff: noqa: D103
"""Tests for schema introspection split modules."""

from __future__ import annotations

import inspect

from datafusion_engine.schema import introspection_cache, introspection_delta


def test_introspection_split_modules_export_functions() -> None:
    assert callable(introspection_cache.catalogs_snapshot)
    assert callable(introspection_delta.constraint_rows)


def test_introspection_split_modules_are_not_core_passthroughs() -> None:
    cache_source = inspect.getsource(introspection_cache)
    delta_source = inspect.getsource(introspection_delta)
    assert "introspection_core as _core" not in cache_source
    assert "introspection_core as _core" not in delta_source
