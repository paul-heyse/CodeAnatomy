"""Tests for semantic execution boundary guards."""

from __future__ import annotations

import pytest
from datafusion import SessionContext

from semantics.registry import SEMANTIC_MODEL
from semantics.semantic_boundary import ensure_semantic_views_registered, is_semantic_view


def test_ensure_semantic_views_registered_raises_for_missing_view() -> None:
    """Missing semantic views raise a ValueError."""
    ctx = SessionContext()
    with pytest.raises(ValueError, match="Semantic views missing"):
        ensure_semantic_views_registered(ctx, view_names=["missing_semantic_view"])


def test_is_semantic_view_matches_registry() -> None:
    """Semantic view lookup respects the naming registry."""
    sample_view = SEMANTIC_MODEL.outputs[0].name
    assert is_semantic_view(sample_view)
    assert not is_semantic_view("non_semantic_view")
