# ruff: noqa: D103
"""Tests for introspection normalization helper cleanup."""

from __future__ import annotations

from pathlib import Path


def test_introspection_core_removed_duplicate_name_normalizers() -> None:
    source = Path("src/datafusion_engine/schema/introspection_core.py").read_text(encoding="utf-8")
    assert "_normalized_aliases" not in source
    assert "_normalized_parameter_names" not in source
