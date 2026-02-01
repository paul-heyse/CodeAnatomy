"""Tests for DataFusion view registry deprecation shims."""

from __future__ import annotations

import pytest

from datafusion_engine import views


def test_view_select_registry_is_removed() -> None:
    """Accessing legacy VIEW_SELECT_REGISTRY raises a deprecation error."""
    with (
        pytest.warns(DeprecationWarning, match="VIEW_SELECT_REGISTRY"),
        pytest.raises(AttributeError),
    ):
        _ = views.VIEW_SELECT_REGISTRY
