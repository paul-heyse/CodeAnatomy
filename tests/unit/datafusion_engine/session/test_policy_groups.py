"""Tests for session policy group defaults."""

from __future__ import annotations

from datafusion_engine.session.policy_groups import (
    CachePolicyGroup,
    DeltaPolicyGroup,
    RuntimeArtifactPolicyGroup,
    SqlPolicyGroup,
)
from datafusion_engine.session.runtime_profile_config import PolicyBundleConfig

EXPECTED_CACHE_MAX_COLUMNS = 64


def test_policy_groups_defaults() -> None:
    """Policy groups should expose stable default values."""
    assert CachePolicyGroup().cache_max_columns == EXPECTED_CACHE_MAX_COLUMNS
    assert SqlPolicyGroup().sql_policy_name == "write"
    assert DeltaPolicyGroup().delta_protocol_mode == "error"
    assert RuntimeArtifactPolicyGroup().runtime_artifact_cache_enabled is False


def test_policy_bundle_group_views() -> None:
    """PolicyBundleConfig should expose focused policy-group views."""
    policy = PolicyBundleConfig()
    assert policy.cache_policy_group().cache_max_columns == EXPECTED_CACHE_MAX_COLUMNS
    assert policy.sql_policy_group().sql_policy_name == "write"
    assert policy.delta_policy_group().delta_protocol_mode == "error"
    assert policy.runtime_artifact_policy_group().runtime_artifact_cache_enabled is False
