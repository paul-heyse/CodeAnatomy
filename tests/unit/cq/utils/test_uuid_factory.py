"""Tests for test_uuid_factory."""

from __future__ import annotations

import uuid

from tools.cq.utils.uuid_factory import (
    artifact_id_hex,
    legacy_compatible_event_id,
    new_uuid7,
    normalize_legacy_identity,
    run_id,
)

UUID7_VERSION = 7
ARTIFACT_HEX_LENGTH = 32


def test_uuid_factory_run_and_artifact_ids_are_sortable() -> None:
    """Generated run ID should be UUID7 and artifact id should be hex-encoded."""
    run_value = run_id()
    artifact_hex = artifact_id_hex()
    parsed = uuid.UUID(run_value)

    assert parsed.version == UUID7_VERSION
    assert len(artifact_hex) == ARTIFACT_HEX_LENGTH


def test_normalize_legacy_identity_is_safe_for_non_v1() -> None:
    """Legacy non-v1 UUIDs should be returned unchanged by normalize helper."""
    value = uuid.uuid4()
    normalized = normalize_legacy_identity(value)
    assert normalized == value


def test_normalize_legacy_identity_handles_v1() -> None:
    """v1 legacy UUIDs should normalize to a safe, comparable UUID output."""
    value = uuid.uuid1()
    normalized = normalize_legacy_identity(value)
    assert isinstance(normalized, uuid.UUID)
    assert normalized.version in {1, 6}


def test_legacy_compatible_event_id_falls_back_safely() -> None:
    """Compatibility helper should produce a valid UUID for event id defaults."""
    value = legacy_compatible_event_id()
    assert isinstance(value, uuid.UUID)
    assert value.version in {6, 7}


def test_new_uuid7_alias() -> None:
    """``new_uuid7`` should emit a UUID7-compatible object."""
    value = new_uuid7()
    assert isinstance(value, uuid.UUID)
    assert value.version == UUID7_VERSION
