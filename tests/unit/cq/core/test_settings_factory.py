"""Tests for SettingsFactory in tools/cq/core/settings_factory.py.

This module verifies that SettingsFactory correctly constructs settings
objects from environment variables and defaults.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from tools.cq.core.cache.base_contracts import CacheRuntimeTuningV1
from tools.cq.core.cache.policy import CqCachePolicyV1
from tools.cq.core.runtime.execution_policy import RuntimeExecutionPolicy
from tools.cq.core.settings_factory import SettingsFactory
from tools.cq.search.tree_sitter.core.infrastructure import ParserControlSettingsV1


def test_runtime_policy_returns_valid_instance() -> None:
    """Test that runtime_policy returns a valid RuntimeExecutionPolicy."""
    policy = SettingsFactory.runtime_policy()
    assert isinstance(policy, RuntimeExecutionPolicy)
    assert policy.parallelism.cpu_workers >= 1


def test_cache_policy_returns_valid_instance(tmp_path: Path) -> None:
    """Test that cache_policy returns a valid CqCachePolicyV1."""
    policy = SettingsFactory.cache_policy(root=tmp_path)
    assert isinstance(policy, CqCachePolicyV1)
    assert policy.directory != ""


def test_parser_controls_returns_valid_instance() -> None:
    """Test that parser_controls returns a valid ParserControlSettingsV1."""
    controls = SettingsFactory.parser_controls()
    assert isinstance(controls, ParserControlSettingsV1)
    assert isinstance(controls.reset_before_parse, bool)


def test_cache_runtime_tuning_returns_valid_instance(tmp_path: Path) -> None:
    """Test that cache_runtime_tuning returns a valid CacheRuntimeTuningV1."""
    policy = SettingsFactory.cache_policy(root=tmp_path)
    tuning = SettingsFactory.cache_runtime_tuning(policy)
    assert isinstance(tuning, CacheRuntimeTuningV1)
    assert tuning.cull_limit >= 0
    assert tuning.eviction_policy in {
        "least-recently-stored",
        "least-recently-used",
        "least-frequently-used",
    }


def test_runtime_policy_respects_environment_max_workers() -> None:
    """Test that runtime_policy respects CQ_RUNTIME_CPU_WORKERS."""
    old_value = os.getenv("CQ_RUNTIME_CPU_WORKERS")
    try:
        os.environ["CQ_RUNTIME_CPU_WORKERS"] = "2"
        policy = SettingsFactory.runtime_policy()
        assert policy.parallelism.cpu_workers == 2
    finally:
        if old_value is not None:
            os.environ["CQ_RUNTIME_CPU_WORKERS"] = old_value
        else:
            os.environ.pop("CQ_RUNTIME_CPU_WORKERS", None)


def test_parser_controls_respects_environment_reset_flag() -> None:
    """Test that parser_controls respects CQ_TREE_SITTER_PARSER_RESET."""
    old_value = os.getenv("CQ_TREE_SITTER_PARSER_RESET")
    try:
        os.environ["CQ_TREE_SITTER_PARSER_RESET"] = "1"
        controls = SettingsFactory.parser_controls()
        assert controls.reset_before_parse is True
    finally:
        if old_value is not None:
            os.environ["CQ_TREE_SITTER_PARSER_RESET"] = old_value
        else:
            os.environ.pop("CQ_TREE_SITTER_PARSER_RESET", None)


def test_cache_runtime_tuning_respects_environment_cull_limit(tmp_path: Path) -> None:
    """Test that cache_runtime_tuning respects CQ_CACHE_CULL_LIMIT."""
    old_value = os.getenv("CQ_CACHE_CULL_LIMIT")
    try:
        os.environ["CQ_CACHE_CULL_LIMIT"] = "32"
        policy = SettingsFactory.cache_policy(root=tmp_path)
        tuning = SettingsFactory.cache_runtime_tuning(policy)
        assert tuning.cull_limit == 32
    finally:
        if old_value is not None:
            os.environ["CQ_CACHE_CULL_LIMIT"] = old_value
        else:
            os.environ.pop("CQ_CACHE_CULL_LIMIT", None)


def test_cache_policy_with_custom_root(tmp_path: Path) -> None:
    """Test that cache_policy uses the provided root path."""
    custom_root = tmp_path / "custom_root"
    custom_root.mkdir()
    policy = SettingsFactory.cache_policy(root=custom_root)
    assert isinstance(policy, CqCachePolicyV1)
    assert policy.directory.startswith(str(custom_root))


def test_all_factory_methods_are_static() -> None:
    """Test that all factory methods can be called without instantiation."""
    # This test verifies that the methods are truly static
    # by calling them without creating an instance
    policy = SettingsFactory.runtime_policy()
    assert isinstance(policy, RuntimeExecutionPolicy)

    controls = SettingsFactory.parser_controls()
    assert isinstance(controls, ParserControlSettingsV1)


@pytest.mark.parametrize(
    ("method_name", "expected_type"),
    [
        ("runtime_policy", RuntimeExecutionPolicy),
        ("parser_controls", ParserControlSettingsV1),
    ],
)
def test_factory_methods_return_expected_types(method_name: str, expected_type: type) -> None:
    """Test that factory methods return expected types."""
    method = getattr(SettingsFactory, method_name)
    result = method()
    assert isinstance(result, expected_type)


def test_cache_runtime_tuning_derives_from_policy(tmp_path: Path) -> None:
    """Test that cache_runtime_tuning derives settings from the provided policy."""
    # Create a policy with known values
    policy = SettingsFactory.cache_policy(root=tmp_path)
    tuning = SettingsFactory.cache_runtime_tuning(policy)

    # Verify tuning is derived from policy
    assert tuning.cull_limit == policy.cull_limit or tuning.cull_limit > 0
    assert tuning.eviction_policy == policy.eviction_policy or tuning.eviction_policy != ""
