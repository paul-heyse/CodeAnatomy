"""Tests for DataFusion view registry contract removals."""

from __future__ import annotations

import pytest

from datafusion_engine import views


def test_view_select_registry_is_removed() -> None:
    """Accessing legacy VIEW_SELECT_REGISTRY raises an attribute error."""
    with pytest.raises(AttributeError):
        _ = views.VIEW_SELECT_REGISTRY
