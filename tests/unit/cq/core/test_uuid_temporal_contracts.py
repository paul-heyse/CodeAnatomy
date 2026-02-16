"""Tests for test_uuid_temporal_contracts."""

from __future__ import annotations

import uuid

from tools.cq.utils.uuid_temporal_contracts import (
    gated_uuid8,
    legacy_event_uuid,
    normalize_external_identity,
    resolve_run_identity_contract,
    temporal_uuid_info,
)

UUID7_VERSION = 7


def test_resolve_run_identity_contract_uses_uuid7_temporal_fields() -> None:
    """Resolve run identity rows using UUID7 temporal metadata."""
    row = resolve_run_identity_contract()
    assert row.run_id
    assert row.run_uuid_version == UUID7_VERSION
    assert row.run_created_ms >= 0


def test_temporal_uuid_info_extracts_uuid7_timestamp() -> None:
    """Parse UUID7 timestamp fields into temporal metadata."""
    row = resolve_run_identity_contract()
    parsed = temporal_uuid_info(uuid.UUID(row.run_id))
    assert parsed.version == UUID7_VERSION
    assert parsed.time_ms is not None
    assert parsed.time_ms >= 0


def test_normalize_external_identity_handles_uuid1_input() -> None:
    """Normalize legacy UUID1 inputs while preserving version compatibility."""
    legacy = uuid.uuid1()
    normalized = normalize_external_identity(legacy)
    assert isinstance(normalized, uuid.UUID)
    assert normalized.version in {1, 6}


def test_gated_uuid8_falls_back_when_disabled() -> None:
    """When gated UUID8 disabled, generation should return UUID7."""
    generated = gated_uuid8(enable_v8=False)
    assert generated.version == UUID7_VERSION


def test_legacy_event_uuid_returns_uuid() -> None:
    """Emit a valid UUID value for legacy event identity compatibility."""
    row = legacy_event_uuid()
    assert isinstance(row, uuid.UUID)
