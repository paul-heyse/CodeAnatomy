"""Schema contract tests for msgspec payloads."""

from __future__ import annotations

import json

import msgspec

from serde_schema_registry import schema_contract_payload
from tests.msgspec_contract._support.goldens import GOLDENS_DIR, assert_text_snapshot
from tests.msgspec_contract._support.models import Envelope
from tests.msgspec_contract._support.normalize import normalize_jsonable


def test_schema_contract_envelope(*, update_goldens: bool) -> None:
    """Snapshot JSON schema for the envelope contract."""
    schema = msgspec.json.schema(Envelope)
    text = json.dumps(normalize_jsonable(schema), indent=2, sort_keys=True)
    assert_text_snapshot(
        path=GOLDENS_DIR / "envelope.schema.json",
        text=text,
        update=update_goldens,
    )


def test_schema_contract_registry(*, update_goldens: bool) -> None:
    """Snapshot JSON schema registry payload."""
    payload = schema_contract_payload()
    text = json.dumps(normalize_jsonable(payload), indent=2, sort_keys=True)
    assert_text_snapshot(
        path=GOLDENS_DIR / "schema_registry.json",
        text=text,
        update=update_goldens,
    )
