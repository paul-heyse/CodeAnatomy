"""Cross-language feature-gate parity hash contract tests."""

from __future__ import annotations

from datafusion_engine.session.runtime_config_policies import (
    DataFusionFeatureGates,
    feature_gate_parity_hash,
    feature_gate_parity_settings,
)

_EXPECTED_DEFAULT_PARITY_HASH = "5c1a4fe3c85cc213eeaf6618f322c3aff311756d645d5c101faec89a8f56a65d"


def test_feature_gate_parity_settings_keys() -> None:
    """Parity settings should expose the canonical feature-gate key set."""
    settings = feature_gate_parity_settings(DataFusionFeatureGates())
    assert sorted(settings) == [
        "datafusion.optimizer.allow_symmetric_joins_without_pruning",
        "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown",
        "datafusion.optimizer.enable_dynamic_filter_pushdown",
        "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
        "datafusion.optimizer.enable_sort_pushdown",
        "datafusion.optimizer.enable_topk_dynamic_filter_pushdown",
    ]


def test_feature_gate_parity_hash_default_contract() -> None:
    """Default feature-gate settings should hash to the pinned contract value."""
    assert feature_gate_parity_hash(DataFusionFeatureGates()) == _EXPECTED_DEFAULT_PARITY_HASH
