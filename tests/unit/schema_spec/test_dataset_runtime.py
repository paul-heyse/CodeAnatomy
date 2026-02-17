"""Tests for dataset runtime helper exports."""

from __future__ import annotations

from schema_spec import dataset_runtime


def test_dataset_runtime_exports() -> None:
    """Expose dataset runtime discovery helpers from module exports."""
    assert "dataset_spec_from_schema" in dataset_runtime.__all__
    assert "resolve_schema_evolution_spec" in dataset_runtime.__all__
