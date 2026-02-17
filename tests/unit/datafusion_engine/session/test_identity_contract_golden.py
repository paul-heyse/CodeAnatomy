"""Golden fixture contract checks for session identity payload hashing."""

from __future__ import annotations

import json
from pathlib import Path

SHA256_HEX_LENGTH = 64


def test_identity_contract_golden_fixture_shape() -> None:
    """Validate required hash fields in the session identity golden fixture."""
    fixture_path = (
        Path(__file__).resolve().parents[4]
        / "tests"
        / "msgspec_contract"
        / "goldens"
        / "session_identity_contract.json"
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    expected = payload["expected"]

    for key in ("spec_hash_hex", "envelope_hash_hex", "rulepack_fingerprint_hex"):
        value = expected[key]
        assert isinstance(value, str)
        assert len(value) == SHA256_HEX_LENGTH
        int(value, 16)
