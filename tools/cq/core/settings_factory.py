"""Canonical factory for environment-derived settings construction.

This module provides a central factory for constructing settings objects
from environment variables and defaults. It consolidates settings construction
logic that was previously scattered across multiple modules.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.cache.base_contracts import CacheRuntimeTuningV1
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.runtime.execution_policy import (
    RuntimeExecutionPolicy,
    default_runtime_execution_policy,
)

if TYPE_CHECKING:
    from tools.cq.search.tree_sitter.core.infrastructure import ParserControlSettingsV1


class SettingsFactory:
    """Canonical factory for environment-derived settings construction.

    This factory provides static methods for constructing various settings
    objects from environment variables and defaults. It serves as a single
    point of access for settings construction across the CQ tool.

    All factory methods are static and stateless, reading from environment
    variables and returning new settings instances on each call.
    """

    @staticmethod
    def runtime_policy() -> RuntimeExecutionPolicy:
        """Construct runtime execution policy from environment.

        Returns:
            RuntimeExecutionPolicy: Resolved runtime execution policy with
                environment overrides applied.
        """
        return default_runtime_execution_policy()

    @staticmethod
    def cache_policy(*, root: Path) -> CqCachePolicyV1:
        """Construct cache policy from environment.

        Parameters:
            root: Repository root path for cache directory resolution.

        Returns:
            CqCachePolicyV1: Resolved cache policy with environment overrides
                applied.
        """
        return default_cache_policy(root=root)

    @staticmethod
    def parser_controls() -> ParserControlSettingsV1:
        """Construct parser control settings from environment.

        Returns:
            ParserControlSettingsV1: Resolved parser control settings with
                environment overrides applied.
        """
        from tools.cq.search.tree_sitter.core.infrastructure import (
            parser_controls_from_env,
        )

        return parser_controls_from_env()

    @staticmethod
    def cache_runtime_tuning(policy: CqCachePolicyV1) -> CacheRuntimeTuningV1:
        """Construct cache runtime tuning from policy and environment.

        Parameters:
            policy: Base cache policy for tuning derivation.

        Returns:
            CacheRuntimeTuningV1: Resolved cache runtime tuning with
                environment overrides applied.
        """
        from tools.cq.core.cache.cache_runtime_tuning import resolve_cache_runtime_tuning

        return resolve_cache_runtime_tuning(policy)


__all__ = ["SettingsFactory"]
