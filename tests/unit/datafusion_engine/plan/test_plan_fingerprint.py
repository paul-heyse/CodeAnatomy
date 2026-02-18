"""Tests for plan fingerprint helper bridge."""

from __future__ import annotations

import pytest

from datafusion_engine.plan import plan_fingerprint

EXPECTED_HASH_CALLS = 2


def test_compute_plan_fingerprint_uses_hash_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fingerprint helper should route through canonical hashing helpers."""
    hash_calls: list[object] = []

    def _fake_hash_msgpack(payload: object) -> str:
        hash_calls.append(payload)
        return "settings-hash" if len(hash_calls) == 1 else "fingerprint"

    monkeypatch.setattr(plan_fingerprint, "hash_msgpack_canonical", _fake_hash_msgpack)
    monkeypatch.setattr(plan_fingerprint, "hash_sha256_hex", lambda _payload: "substrait-hash")

    result = plan_fingerprint.compute_plan_fingerprint(
        plan_fingerprint.PlanFingerprintInputs(
            substrait_bytes=b"substrait",
            df_settings={"key": "value"},
            planning_env_hash="planning",
            rulepack_hash="rulepack",
            information_schema_hash="info",
            udf_snapshot_hash="udf",
            required_udfs=("udf_b", "udf_a"),
            required_rewrite_tags=("tag_b", "tag_a"),
            delta_inputs=(),
            delta_store_policy_hash="store",
        )
    )

    assert result == "fingerprint"
    assert len(hash_calls) == EXPECTED_HASH_CALLS
