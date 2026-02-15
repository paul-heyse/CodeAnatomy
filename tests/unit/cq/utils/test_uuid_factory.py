from __future__ import annotations

import uuid

from tools.cq.utils.uuid_factory import (
    artifact_id_hex,
    legacy_compatible_event_id,
    new_uuid7,
    normalize_legacy_identity,
    run_id,
)


def test_uuid_factory_run_and_artifact_ids_are_sortable() -> None:
    run_value = run_id()
    artifact_hex = artifact_id_hex()
    parsed = uuid.UUID(run_value)

    assert parsed.version == 7
    assert len(artifact_hex) == 32


def test_normalize_legacy_identity_is_safe_for_non_v1() -> None:
    value = uuid.uuid4()
    normalized = normalize_legacy_identity(value)
    assert normalized == value


def test_normalize_legacy_identity_handles_v1() -> None:
    value = uuid.uuid1()
    normalized = normalize_legacy_identity(value)
    assert isinstance(normalized, uuid.UUID)
    assert normalized.version in {1, 6}


def test_legacy_compatible_event_id_falls_back_safely() -> None:
    value = legacy_compatible_event_id()
    assert isinstance(value, uuid.UUID)
    assert value.version in {6, 7}


def test_new_uuid7_alias() -> None:
    value = new_uuid7()
    assert isinstance(value, uuid.UUID)
    assert value.version == 7
