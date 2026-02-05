from __future__ import annotations

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from hamilton_pipeline import driver_factory


def test_diskcache_profile_overrides() -> None:
    profile = DataFusionRuntimeProfile()
    config = {
        "datafusion_cache": {
            "cache_policy": {
                "listing_cache_size": 123,
                "metadata_cache_size": 456,
                "stats_cache_size": 789,
            },
            "diskcache_profile": {
                "overrides": {"schema": {"size_limit_bytes": 1024}},
                "ttl_seconds": {"schema": 42.0},
            },
        }
    }

    updated = driver_factory._apply_datafusion_cache_config(  # noqa: SLF001
        profile,
        config=config,
    )
    diskcache_profile = updated.policies.diskcache_profile
    assert diskcache_profile is not None
    assert diskcache_profile.settings_for("schema").size_limit_bytes == 1024
    assert diskcache_profile.ttl_for("schema") == 42.0

    cache_policy = updated.policies.cache_policy
    assert cache_policy is not None
    assert cache_policy.listing_cache_size == 123
    assert cache_policy.metadata_cache_size == 456
    assert cache_policy.stats_cache_size == 789
