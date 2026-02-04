"""Adapter to build SemanticRuntimeConfig from DataFusionRuntimeProfile.

This module inverts the dependency direction, allowing semantics to own its
runtime configuration while datafusion_engine provides the adapter to construct
it from the DataFusion-native runtime profile.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeGuard

from semantics.runtime import CachePolicy, SemanticRuntimeConfig

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def _is_cache_policy(value: object) -> TypeGuard[CachePolicy]:
    if not isinstance(value, str):
        return False
    return value in {"none", "delta_staging", "delta_output"}


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
    from semantics.registry import SEMANTIC_MODEL

    view_names = [spec.name for spec in SEMANTIC_MODEL.outputs]
    output_locations = {
        name: str(location.path)
        for name in view_names
        if (location := profile.dataset_location(name)) is not None
    }

    # Extract cache policy overrides
    cache_overrides: dict[str, CachePolicy] = {}
    semantic_cache = profile.data_sources.semantic_cache_overrides
    cache_overrides.update(
        {name: policy for name, policy in semantic_cache.items() if _is_cache_policy(policy)}
    )

    # Extract CDF configuration
    cdf_enabled = profile.data_sources.cdf_enabled
    cdf_cursor_store = profile.data_sources.cdf_cursor_store

    # Extract storage options (prefer delta store policy settings)
    storage_options = None
    store_policy = profile.policies.delta_store_policy
    if store_policy is not None:
        storage_options = dict(store_policy.storage_options)

    # Extract schema evolution setting
    schema_evolution_enabled = profile.features.enable_schema_evolution_adapter

    return SemanticRuntimeConfig(
        output_locations=output_locations,
        cache_policy_overrides=cache_overrides,
        cdf_enabled=bool(cdf_enabled),
        cdf_cursor_store=cdf_cursor_store,
        storage_options=dict(storage_options) if storage_options else None,
        schema_evolution_enabled=bool(schema_evolution_enabled),
    )


def apply_semantic_runtime_config(
    profile: DataFusionRuntimeProfile,
    semantic_config: SemanticRuntimeConfig,
) -> DataFusionRuntimeProfile:
    """Apply SemanticRuntimeConfig to a DataFusion runtime profile.

    This function applies semantic-defined configuration back to a DataFusion
    profile, ensuring semantic settings take precedence over profile defaults.

    Parameters
    ----------
    profile
        DataFusion runtime profile to update.
    semantic_config
        Semantic runtime configuration to apply.

    Returns
    -------
    DataFusionRuntimeProfile
        Updated profile with semantic settings applied.
    """
    from dataclasses import replace

    from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation

    # Build updated semantic output locations from config
    semantic_output_locations: dict[str, DatasetLocation] = dict(
        profile.data_sources.semantic_output_locations
    )
    for name, path in semantic_config.output_locations.items():
        if name not in semantic_output_locations:
            semantic_output_locations[name] = DatasetLocation(
                path=path,
                format="delta",
                storage_options=dict(semantic_config.storage_options or {}),
            )

    # Update registry catalog if semantic_output_catalog_name is set
    registry_catalogs = dict(profile.catalog.registry_catalogs)
    if profile.data_sources.semantic_output_catalog_name:
        catalog_name = profile.data_sources.semantic_output_catalog_name
        existing_catalog = registry_catalogs.get(catalog_name)
        if existing_catalog is None:
            existing_catalog = DatasetCatalog()
        for name, path in semantic_config.output_locations.items():
            if existing_catalog.get(name) is None:
                existing_catalog.register(
                    name,
                    DatasetLocation(
                        path=path,
                        format="delta",
                        storage_options=dict(semantic_config.storage_options or {}),
                    ),
                )
        registry_catalogs[catalog_name] = existing_catalog

    # Apply cache policy overrides - semantic config is authoritative
    semantic_cache_overrides = dict(profile.data_sources.semantic_cache_overrides or {})
    semantic_cache_overrides.update(semantic_config.cache_policy_overrides)

    return replace(
        profile,
        data_sources=replace(
            profile.data_sources,
            semantic_output_locations=semantic_output_locations,
            semantic_cache_overrides=semantic_cache_overrides,
            cdf_enabled=semantic_config.cdf_enabled,
            cdf_cursor_store=semantic_config.cdf_cursor_store,
        ),
        catalog=replace(
            profile.catalog,
            registry_catalogs=registry_catalogs,
        ),
        features=replace(
            profile.features,
            enable_schema_evolution_adapter=semantic_config.schema_evolution_enabled,
        ),
    )


__all__ = [
    "apply_semantic_runtime_config",
    "semantic_runtime_from_profile",
]
