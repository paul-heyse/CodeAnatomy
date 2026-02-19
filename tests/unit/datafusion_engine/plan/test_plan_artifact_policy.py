"""Plan artifact serialization policy tests."""

from __future__ import annotations

import pytest

from datafusion_engine.plan.artifact_serialization import proto_msgpack
from datafusion_engine.plan.contracts import PlanArtifactPolicyV1
from datafusion_engine.plan.plan_fingerprint import (
    PlanFingerprintInputs,
    compute_plan_fingerprint,
)


def test_proto_msgpack_rejects_cross_process_proto_by_default() -> None:
    """Cross-process proto serialization is rejected under default policy."""
    with pytest.raises(ValueError, match="cross-process artifacts must use Substrait"):
        proto_msgpack({"k": "v"}, cross_process=True, policy=PlanArtifactPolicyV1())


def test_compute_plan_fingerprint_requires_substrait_bytes() -> None:
    """Fingerprinting fails when Substrait bytes are empty."""
    inputs = PlanFingerprintInputs(
        substrait_bytes=b"",
        df_settings={},
        planning_env_hash=None,
        rulepack_hash=None,
        information_schema_hash=None,
        udf_snapshot_hash="abc",
        required_udfs=(),
        required_rewrite_tags=(),
        delta_inputs=(),
        delta_store_policy_hash=None,
    )
    with pytest.raises(ValueError, match="non-empty Substrait bytes"):
        compute_plan_fingerprint(inputs)
