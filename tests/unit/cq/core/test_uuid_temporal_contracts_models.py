from __future__ import annotations

from tools.cq.utils.uuid_temporal_contracts_models import RunIdentityContractV1, TemporalUuidInfoV1


def test_temporal_uuid_info_model() -> None:
    row = TemporalUuidInfoV1(
        value="00000000-0000-7000-8000-000000000000",
        version=7,
        variant="rfc_4122",
        time_ms=123,
    )
    assert row.version == 7
    assert row.time_ms == 123


def test_run_identity_contract_model() -> None:
    row = RunIdentityContractV1(
        run_id="00000000-0000-7000-8000-000000000000",
        run_uuid_version=7,
        run_variant="rfc_4122",
        run_created_ms=456,
    )
    assert row.run_uuid_version == 7
    assert row.run_created_ms == 456
