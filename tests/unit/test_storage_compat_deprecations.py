"""Decommission coverage for removed storage compatibility exports."""

from __future__ import annotations

import warnings

import pytest


def test_storage_deltalake_query_delta_sql_removed() -> None:
    """Legacy query_delta_sql export should be fully removed."""
    import storage.deltalake as deltalake_module

    deltalake_module.__dict__.pop("query_delta_sql", None)
    with pytest.raises(AttributeError):
        _ = deltalake_module.query_delta_sql


def test_storage_deltalake_disable_feature_removed() -> None:
    """Legacy disable feature helpers should no longer be exported."""
    import storage.deltalake as deltalake_module

    name = "disable_delta_change_data_feed"
    deltalake_module.__dict__.pop(name, None)
    with pytest.raises(AttributeError):
        _ = getattr(deltalake_module, name)


def test_storage_top_level_delta_export_removed() -> None:
    """Top-level storage compatibility exports should no longer exist."""
    import storage as storage_module

    name = "DeltaCdfOptions"
    storage_module.__dict__.pop(name, None)
    with pytest.raises(AttributeError):
        _ = getattr(storage_module, name)


def test_storage_deltalake_canonical_export_no_warning() -> None:
    """Canonical non-legacy exports should not emit deprecation warnings."""
    import storage.deltalake as deltalake_module

    name = "canonical_table_uri"
    deltalake_module.__dict__.pop(name, None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = getattr(deltalake_module, name)
    assert callable(value)
    assert all(not issubclass(item.category, DeprecationWarning) for item in caught)


def test_storage_canonical_submodule_imports() -> None:
    """Root storage package supports canonical submodule imports only."""
    import storage

    assert storage.__all__ == ()
