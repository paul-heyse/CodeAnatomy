"""Session and runtime management."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.bootstrap.zero_row import (
        ZeroRowBootstrapReport,
        ZeroRowBootstrapRequest,
        ZeroRowDatasetPlan,
    )
    from datafusion_engine.session.features import (
        FeatureStateSnapshot,
        feature_state_snapshot,
        named_args_supported,
    )
    from datafusion_engine.session.introspection import (
        collect_datafusion_metrics,
        collect_datafusion_traces,
        evict_diskcache_entries,
        register_cdf_inputs_for_profile,
        run_diskcache_maintenance,
        schema_introspector_for_profile,
    )
    from datafusion_engine.session.runtime import (
        DataFusionRuntimeProfile,
        ZeroRowBootstrapConfig,
    )

__all__ = [
    "DataFusionRuntimeProfile",
    "FeatureStateSnapshot",
    "ZeroRowBootstrapConfig",
    "ZeroRowBootstrapReport",
    "ZeroRowBootstrapRequest",
    "ZeroRowDatasetPlan",
    "collect_datafusion_metrics",
    "collect_datafusion_traces",
    "evict_diskcache_entries",
    "feature_state_snapshot",
    "named_args_supported",
    "register_cdf_inputs_for_profile",
    "run_diskcache_maintenance",
    "schema_introspector_for_profile",
]

# ---------------------------------------------------------------------------
# Lazy attribute resolution keyed by canonical source module.
# ---------------------------------------------------------------------------

_RUNTIME_NAMES: frozenset[str] = frozenset(
    {
        "DataFusionRuntimeProfile",
        "ZeroRowBootstrapConfig",
    }
)

_BOOTSTRAP_NAMES: frozenset[str] = frozenset(
    {
        "ZeroRowBootstrapReport",
        "ZeroRowBootstrapRequest",
        "ZeroRowDatasetPlan",
    }
)

_FEATURES_NAMES: frozenset[str] = frozenset(
    {
        "FeatureStateSnapshot",
        "feature_state_snapshot",
        "named_args_supported",
    }
)

_INTROSPECTION_NAMES: frozenset[str] = frozenset(
    {
        "collect_datafusion_metrics",
        "collect_datafusion_traces",
        "evict_diskcache_entries",
        "register_cdf_inputs_for_profile",
        "run_diskcache_maintenance",
        "schema_introspector_for_profile",
    }
)


def __getattr__(name: str) -> object:
    if name in _RUNTIME_NAMES:
        from datafusion_engine.session.runtime import (
            DataFusionRuntimeProfile,
            ZeroRowBootstrapConfig,
        )

        runtime_exports = {
            "DataFusionRuntimeProfile": DataFusionRuntimeProfile,
            "ZeroRowBootstrapConfig": ZeroRowBootstrapConfig,
        }
        return runtime_exports[name]
    if name in _BOOTSTRAP_NAMES:
        from datafusion_engine.bootstrap.zero_row import (
            ZeroRowBootstrapReport,
            ZeroRowBootstrapRequest,
            ZeroRowDatasetPlan,
        )

        bootstrap_exports = {
            "ZeroRowBootstrapReport": ZeroRowBootstrapReport,
            "ZeroRowBootstrapRequest": ZeroRowBootstrapRequest,
            "ZeroRowDatasetPlan": ZeroRowDatasetPlan,
        }
        return bootstrap_exports[name]
    if name in _FEATURES_NAMES:
        from datafusion_engine.session.features import (
            FeatureStateSnapshot,
            feature_state_snapshot,
            named_args_supported,
        )

        features_exports = {
            "FeatureStateSnapshot": FeatureStateSnapshot,
            "feature_state_snapshot": feature_state_snapshot,
            "named_args_supported": named_args_supported,
        }
        return features_exports[name]
    if name in _INTROSPECTION_NAMES:
        from datafusion_engine.session.introspection import (
            collect_datafusion_metrics,
            collect_datafusion_traces,
            evict_diskcache_entries,
            register_cdf_inputs_for_profile,
            run_diskcache_maintenance,
            schema_introspector_for_profile,
        )

        introspection_exports = {
            "collect_datafusion_metrics": collect_datafusion_metrics,
            "collect_datafusion_traces": collect_datafusion_traces,
            "evict_diskcache_entries": evict_diskcache_entries,
            "register_cdf_inputs_for_profile": register_cdf_inputs_for_profile,
            "run_diskcache_maintenance": run_diskcache_maintenance,
            "schema_introspector_for_profile": schema_introspector_for_profile,
        }
        return introspection_exports[name]
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
