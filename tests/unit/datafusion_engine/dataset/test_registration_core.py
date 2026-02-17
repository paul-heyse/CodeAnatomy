# ruff: noqa: D103
"""Tests for dataset registration core surface."""

from __future__ import annotations

from datafusion_engine.dataset import registration_core


def test_registration_core_has_context_registration_helper() -> None:
    assert hasattr(registration_core, "_register_dataset_with_context")
