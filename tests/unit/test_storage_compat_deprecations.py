"""Compatibility deprecation coverage for storage facade exports."""

from __future__ import annotations

import warnings


def test_storage_deltalake_query_delta_sql_warns() -> None:
    """Legacy query_delta_sql export should emit a deprecation warning."""
    import storage.deltalake as deltalake_module

    deltalake_module.__dict__.pop("query_delta_sql", None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = deltalake_module.query_delta_sql
    assert callable(value)
    assert any(
        issubclass(item.category, DeprecationWarning) and "query_delta_sql" in str(item.message)
        for item in caught
    )


def test_storage_deltalake_disable_feature_warns() -> None:
    """Legacy disable feature helpers should emit a deprecation warning."""
    import storage.deltalake as deltalake_module

    name = "disable_delta_change_data_feed"
    deltalake_module.__dict__.pop(name, None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = getattr(deltalake_module, name)
    assert callable(value)
    assert any(
        issubclass(item.category, DeprecationWarning) and name in str(item.message)
        for item in caught
    )


def test_storage_top_level_delta_export_warns() -> None:
    """Top-level storage delta compatibility exports should warn."""
    import storage as storage_module

    name = "DeltaCdfOptions"
    storage_module.__dict__.pop(name, None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = getattr(storage_module, name)
    assert value is not None
    assert any(
        issubclass(item.category, DeprecationWarning) and "DeltaCdfOptions" in str(item.message)
        for item in caught
    )


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
