"""Tests for test_uuid_temporal_contracts_models."""

from __future__ import annotations

from tools.cq.utils.uuid_temporal_contracts import RunIdentityContractV1, TemporalUuidInfoV1

UUID7_VERSION = 7
TEMPORAL_TIME_MS = 123
RUN_CREATED_MS = 456


def test_temporal_uuid_info_model() -> None:
    """Validate model field typing and defaults for temporal UUID metadata."""
    row = TemporalUuidInfoV1(
        value="00000000-0000-7000-8000-000000000000",
        version=UUID7_VERSION,
        variant="rfc_4122",
        time_ms=TEMPORAL_TIME_MS,
    )
    assert row.version == UUID7_VERSION
    assert row.time_ms == TEMPORAL_TIME_MS


def test_run_identity_contract_model() -> None:
    """Validate model fields for run identity contract."""
    row = RunIdentityContractV1(
        run_id="00000000-0000-7000-8000-000000000000",
        run_uuid_version=UUID7_VERSION,
        run_variant="rfc_4122",
        run_created_ms=RUN_CREATED_MS,
    )
    assert row.run_uuid_version == UUID7_VERSION
    assert row.run_created_ms == RUN_CREATED_MS
