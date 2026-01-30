"""Tests for extended validation helpers."""

from __future__ import annotations

import pytest

from utils.validation import ensure_not_empty, ensure_subset, ensure_unique


def test_ensure_not_empty_accepts() -> None:
    """Ensure ensure_not_empty returns non-empty values."""
    values = [1, 2]
    assert ensure_not_empty(values, label="values") is values


def test_ensure_not_empty_rejects() -> None:
    """Ensure ensure_not_empty rejects empty sequences."""
    with pytest.raises(ValueError, match="values must not be empty"):
        ensure_not_empty([], label="values")


def test_ensure_subset() -> None:
    """Ensure ensure_subset validates membership."""
    ensure_subset(["a"], {"a", "b"}, label="items")
    with pytest.raises(ValueError, match="items contains invalid values"):
        ensure_subset(["a", "c"], {"a", "b"}, label="items")


def test_ensure_unique() -> None:
    """Ensure ensure_unique rejects duplicates and returns deduped list."""
    assert ensure_unique(["a", "b"], label="items") == ["a", "b"]
    with pytest.raises(ValueError, match="items contains duplicates"):
        ensure_unique(["a", "a"], label="items")
