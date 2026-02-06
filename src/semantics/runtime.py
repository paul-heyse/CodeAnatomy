"""Semantic runtime configuration types.

This module defines runtime configuration types owned by the semantics module.
DataFusion engine provides adapters to construct these configurations from
DataFusionRuntimeProfile, inverting the dependency direction so that semantics
does not depend on datafusion_engine runtime internals.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from semantics.incremental.cdf_cursors import CdfCursorStore

CachePolicy = Literal["none", "delta_staging", "delta_output"]


@dataclass(frozen=True)
class SemanticRuntimeConfig:
    """Semantic-specific runtime configuration.

    Owned by semantics module. DataFusion engine provides adapters
    to build this from DataFusionRuntimeProfile.

    Attributes:
    ----------
    output_locations
        Mapping of dataset names to output paths.
    cache_policy_overrides
        Overrides for cache policy by view name.
    cdf_enabled
        Whether Change Data Feed processing is enabled.
    cdf_cursor_store
        Optional CDF cursor store for incremental processing.
    storage_options
        Storage options for Delta Lake access.
    schema_evolution_enabled
        Whether schema evolution adapters are enabled.
    """

    output_locations: Mapping[str, str] = field(default_factory=dict)
    cache_policy_overrides: Mapping[str, CachePolicy] = field(default_factory=dict)
    cdf_enabled: bool = False
    cdf_cursor_store: CdfCursorStore | None = None
    storage_options: Mapping[str, str] | None = None
    schema_evolution_enabled: bool = True

    def output_path(self, name: str) -> str | None:
        """Return output path for a dataset name.

        Parameters
        ----------
        name
            Dataset name.

        Returns:
        -------
        str | None
            Output path if configured.
        """
        return self.output_locations.get(name)

    def cache_policy(self, name: str, *, default: CachePolicy = "none") -> CachePolicy:
        """Return cache policy for a view name.

        Parameters
        ----------
        name
            View name.
        default
            Default policy if not overridden.

        Returns:
        -------
        CachePolicy
            Cache policy for the view.
        """
        return self.cache_policy_overrides.get(name, default)


@dataclass(frozen=True)
class SemanticBuildOptions:
    """Options for semantic build operations.

    Attributes:
    ----------
    use_cdf
        Whether to enable CDF-aware incremental joins.
    validate_inputs
        Whether to validate input schemas.
    collect_metrics
        Whether to collect operation metrics.
    trace_fingerprints
        Whether to trace plan fingerprints.
    """

    use_cdf: bool = False
    validate_inputs: bool = True
    collect_metrics: bool = True
    trace_fingerprints: bool = True


__all__ = [
    "CachePolicy",
    "SemanticBuildOptions",
    "SemanticRuntimeConfig",
]
