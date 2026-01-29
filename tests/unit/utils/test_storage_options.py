"""Tests for storage option normalization helpers."""

from __future__ import annotations

from utils.storage_options import merged_storage_options, normalize_storage_options


def test_normalize_storage_options_basic() -> None:
    """Normalize storage options and preserve string conversions."""
    storage, log_storage = normalize_storage_options({"a": 1}, {"b": True})
    assert storage == {"a": "1"}
    assert log_storage == {"b": "True"}


def test_normalize_storage_options_empty() -> None:
    """Return None values when no options are provided."""
    storage, log_storage = normalize_storage_options(None, None)
    assert storage is None
    assert log_storage is None


def test_normalize_storage_options_fallback() -> None:
    """Fallback log storage to storage options when requested."""
    storage, log_storage = normalize_storage_options({"x": "1"}, None, fallback_log_to_storage=True)
    assert storage == {"x": "1"}
    assert log_storage == {"x": "1"}


def test_merged_storage_options() -> None:
    """Merge storage and log storage options deterministically."""
    merged = merged_storage_options({"a": 1}, {"b": "2"})
    assert merged == {"a": "1", "b": "2"}
