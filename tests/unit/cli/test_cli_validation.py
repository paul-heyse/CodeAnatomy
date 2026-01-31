"""Tests for CLI configuration validation."""

from __future__ import annotations

import pytest

from cli.validation import validate_config_mutual_exclusion
from core_types import JsonValue


def test_validate_config_mutual_exclusion_restore_conflict() -> None:
    """Ensure restore config rejects simultaneous version/timestamp."""
    payload: dict[str, JsonValue] = {
        "delta": {"restore": {"version": 1, "timestamp": "2024-01-01"}}
    }
    with pytest.raises(ValueError, match="delta\\.restore"):
        validate_config_mutual_exclusion(payload)


def test_validate_config_mutual_exclusion_export_conflict() -> None:
    """Ensure export config rejects simultaneous version/timestamp."""
    payload: dict[str, JsonValue] = {"delta": {"export": {"version": 2, "timestamp": "2024-01-02"}}}
    with pytest.raises(ValueError, match="delta\\.export"):
        validate_config_mutual_exclusion(payload)
