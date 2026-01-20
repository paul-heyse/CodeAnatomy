"""Tests for relspec policy registry resolution."""

import pytest

from relspec.model import AmbiguityPolicy, ConfidencePolicy
from relspec.policies import PolicyRegistry


def test_policy_registry_resolves_cpg_confidence() -> None:
    """Resolve a known CPG confidence policy."""
    registry = PolicyRegistry()
    policy = registry.resolve_confidence("cpg", "scip")
    assert isinstance(policy, ConfidencePolicy)
    assert policy.base == 1.0


def test_policy_registry_resolves_normalize_ambiguity() -> None:
    """Resolve a known normalize ambiguity policy."""
    registry = PolicyRegistry()
    policy = registry.resolve_ambiguity("normalize", "preserve")
    assert isinstance(policy, AmbiguityPolicy)


def test_policy_registry_returns_none_for_missing_name() -> None:
    """Return None when the policy name is absent."""
    registry = PolicyRegistry()
    assert registry.resolve_confidence("cpg", None) is None
    assert registry.resolve_ambiguity("normalize", None) is None
    assert registry.resolve_confidence("extract", None) is None


def test_policy_registry_raises_on_unknown_policy() -> None:
    """Raise when a policy name is unknown."""
    registry = PolicyRegistry()
    with pytest.raises(KeyError):
        registry.resolve_confidence("cpg", "missing")
    with pytest.raises(KeyError):
        registry.resolve_ambiguity("normalize", "missing")
