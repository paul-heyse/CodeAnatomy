"""Plan identity hashing tests for listing partition inference settings."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

from datafusion_engine.plan.plan_identity import PlanIdentityInputs, plan_identity_payload
from utils.hashing import hash_json_default

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_artifacts import PlanArtifacts


class _FakeProfile:
    @staticmethod
    def settings_hash() -> str:
        return "settings-hash"

    @staticmethod
    def context_cache_key() -> str:
        return "context-key"


def _identity_hash_for_listing_setting(value: str) -> str:
    artifacts = SimpleNamespace(
        udf_snapshot_hash="udf-hash",
        function_registry_hash="registry-hash",
        domain_planner_names=(),
        df_settings={
            "datafusion.execution.listing_table_factory_infer_partitions": value,
        },
        planning_env_hash="env-hash",
        planning_env_snapshot={
            "planning_surface_policy_version": "v1",
            "planning_surface_policy_hash": "policy-hash",
        },
        rulepack_hash="rule-hash",
        information_schema_hash="info-hash",
    )
    payload = plan_identity_payload(
        PlanIdentityInputs(
            plan_fingerprint="plan-hash",
            artifacts=cast("PlanArtifacts", artifacts),
            required_udfs=(),
            required_rewrite_tags=(),
            delta_inputs=(),
            scan_units=(),
            profile=cast("DataFusionRuntimeProfile", _FakeProfile()),
        )
    )
    return hash_json_default(payload, str_keys=True)


def test_listing_partition_inference_changes_identity_hash() -> None:
    """Identity hash must include listing partition inference policy."""
    disabled_hash = _identity_hash_for_listing_setting("false")
    enabled_hash = _identity_hash_for_listing_setting("true")

    assert disabled_hash != enabled_hash
