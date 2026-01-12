"""Tests for shared provenance constants."""

from __future__ import annotations

from arrowdsl.kernels import provenance_sort_keys
from schema_spec.fields import PROVENANCE_COLS


def test_provenance_column_names_are_shared() -> None:
    """Ensure provenance columns are shared between schema and kernels."""
    assert tuple(PROVENANCE_COLS) == tuple(provenance_sort_keys())
