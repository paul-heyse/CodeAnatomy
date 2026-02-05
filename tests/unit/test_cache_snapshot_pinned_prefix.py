from __future__ import annotations

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    PolicyBundleConfig,
    cache_prefix_for_delta_snapshot,
)


def test_cache_prefix_for_delta_snapshot() -> None:
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(snapshot_pinned_mode="delta_version"),
    )
    prefix = cache_prefix_for_delta_snapshot(
        profile,
        dataset_name="events",
        snapshot={"version": 12},
    )
    assert prefix is not None
    assert prefix.endswith("::events::12")
    assert prefix.startswith(profile.context_cache_key())
