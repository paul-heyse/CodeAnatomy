"""Adapter to build SemanticRuntimeConfig from DataFusionRuntimeProfile.

This module inverts the dependency direction, allowing semantics to own its
runtime configuration while datafusion_engine provides the adapter to construct
it from the DataFusion-native runtime profile.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from semantics.runtime import CachePolicy, SemanticRuntimeConfig

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def semantic_runtime_from_profile(
    profile: DataFusionRuntimeProfile,
) -> SemanticRuntimeConfig:
    """Build SemanticRuntimeConfig from DataFusionRuntimeProfile.

    Extracts semantic-relevant configuration from the DataFusion runtime
    profile and constructs a SemanticRuntimeConfig that can be passed to
    semantic operations without coupling them to DataFusion internals.

    Parameters
    ----------
    profile
        DataFusion runtime profile with configuration.

    Returns
    -------
    SemanticRuntimeConfig
        Semantic runtime configuration extracted from profile.
    """
    # Extract output locations from profile using canonical view names
    from relspec.view_defs import RELATION_OUTPUT_NAME
    from semantics.naming import SEMANTIC_VIEW_NAMES

    output_locations: dict[str, str] = {}
    view_names = list(SEMANTIC_VIEW_NAMES)
    if RELATION_OUTPUT_NAME not in view_names:
        view_names.append(RELATION_OUTPUT_NAME)
    for name in view_names:
        location = profile.dataset_location(name)
        if location is None:
            continue
        output_locations[name] = str(location.path)

    # Extract cache policy overrides
    cache_overrides: dict[str, CachePolicy] = {}
    semantic_cache = getattr(profile, "semantic_cache_overrides", None)
    if semantic_cache is not None:
        valid_policies = {"none", "delta_staging", "delta_output"}
        cache_overrides.update(
            {
                name: policy  # type: ignore[misc]
                for name, policy in semantic_cache.items()
                if policy in valid_policies
            }
        )

    # Extract CDF configuration
    cdf_enabled = getattr(profile, "cdf_enabled", False)
    cdf_cursor_store = getattr(profile, "cdf_cursor_store", None)

    # Extract storage options (prefer delta store policy settings)
    storage_options = None
    store_policy = getattr(profile, "delta_store_policy", None)
    if store_policy is not None:
        storage_options = dict(store_policy.storage_options)

    # Extract schema evolution setting
    schema_evolution_enabled = getattr(profile, "enable_schema_evolution_adapter", True)

    return SemanticRuntimeConfig(
        output_locations=output_locations,
        cache_policy_overrides=cache_overrides,
        cdf_enabled=bool(cdf_enabled),
        cdf_cursor_store=cdf_cursor_store,
        storage_options=dict(storage_options) if storage_options else None,
        schema_evolution_enabled=bool(schema_evolution_enabled),
    )


__all__ = [
    "semantic_runtime_from_profile",
]
