"""Tests for Ibis parameter registry behavior."""

from __future__ import annotations

import pytest

from ibis_engine.params_bridge import IbisParamRegistry, ScalarParamSpec

DEFAULT_ALPHA = 5


def test_param_registry_applies_defaults() -> None:
    """Apply default values for optional parameters."""
    registry = IbisParamRegistry()
    alpha = registry.register(
        ScalarParamSpec(
            name="alpha",
            dtype="int64",
            default=DEFAULT_ALPHA,
        )
    )
    beta = registry.register(ScalarParamSpec(name="beta", dtype="string", required=True))
    bindings = registry.bindings({"beta": "ok"})
    assert bindings[alpha] == DEFAULT_ALPHA
    assert bindings[beta] == "ok"


def test_param_registry_requires_missing_values() -> None:
    """Reject missing required parameter values."""
    registry = IbisParamRegistry()
    registry.register(ScalarParamSpec(name="alpha", dtype="int64", required=True))
    with pytest.raises(ValueError, match="Missing parameter values"):
        registry.bindings({})
