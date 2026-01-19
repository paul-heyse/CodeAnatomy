"""Unit tests for parameter-aware cache policy gating."""

from __future__ import annotations

import ibis
import pytest
from ibis.expr.types import Value

from arrowdsl.core.context import execution_context_factory
from engine.materialize import resolve_cache_policy
from engine.plan_policy import ExecutionSurfacePolicy
from ibis_engine.runner import validate_plan_params


def test_cache_policy_disabled_with_params() -> None:
    """Disable caching when parameter bindings are provided."""
    ctx = execution_context_factory("default")
    policy = ExecutionSurfacePolicy()
    param: Value = ibis.param(ibis.dtype("int64")).name("limit")
    cache_policy = resolve_cache_policy(
        ctx=ctx,
        policy=policy,
        prefer_reader=False,
        params={param: 1},
    )
    assert cache_policy.enabled is False
    assert cache_policy.param_mode == "typed"
    assert cache_policy.param_signature is not None


def test_plan_rejects_unbound_param_names() -> None:
    """Require Ibis parameter bindings for plan execution."""
    with pytest.raises(ValueError, match="parameter bindings"):
        _ = validate_plan_params({"limit": 1})
