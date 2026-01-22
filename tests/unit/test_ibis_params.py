"""Tests for Ibis parameter registry behavior."""

from __future__ import annotations

import ibis
import pytest

from ibis_engine.params_bridge import (
    IbisParamRegistry,
    JoinOptions,
    ScalarParamSpec,
    list_param_join,
)

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


def test_list_param_join_applies_collision_policy() -> None:
    """Apply name collision policy for list-param joins."""
    left = ibis.memtable({"id": [1], "value": ["a"]})
    right = ibis.memtable({"id": [1], "value": ["b"]})
    result = list_param_join(
        left,
        param_table=right,
        left_col="id",
        right_col="id",
        options=JoinOptions(how="inner", lname="{name}_l", rname="{name}_r"),
    )
    assert result.schema().names == ("id", "value_l", "value_r")
