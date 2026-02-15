from __future__ import annotations

from tools.cq.core.contracts import UuidIdentityContractV1


def test_uuid_identity_contract_defaults() -> None:
    row = UuidIdentityContractV1(run_id="run-1", artifact_id="artifact-1")
    assert row.run_id == "run-1"
    assert row.artifact_id == "artifact-1"
    assert row.cache_key_uses_uuid is False
