"""Tests for normalize registry validation."""

from __future__ import annotations

from normalize.registry_validation import validate_rule_specs
from normalize.rule_registry import normalize_rules


def test_normalize_registry_validation() -> None:
    """Ensure normalize rules validate against registry contracts."""
    validate_rule_specs(normalize_rules())
