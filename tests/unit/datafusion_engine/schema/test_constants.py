"""Tests for schema constants metadata contracts."""

from __future__ import annotations

from datafusion_engine.arrow.metadata import required_functions_from_metadata
from datafusion_engine.schema.constants import ENGINE_FUNCTION_REQUIREMENTS


def test_engine_function_requirements_metadata_present() -> None:
    """Engine function requirements metadata is present and keyed."""
    assert ENGINE_FUNCTION_REQUIREMENTS
    assert b"required_functions" in ENGINE_FUNCTION_REQUIREMENTS


def test_engine_function_requirements_metadata_contains_stable_id() -> None:
    """Engine function metadata contains stable_id requirement."""
    required = required_functions_from_metadata(ENGINE_FUNCTION_REQUIREMENTS)
    assert "stable_id" in required
