"""Tests for rust fact payload contracts."""

from __future__ import annotations

from tools.cq.search.rust.evidence import RustFactPayloadV1, coerce_fact_payload


def test_coerce_fact_payload() -> None:
    payload = coerce_fact_payload(
        {
            "scope_chain": ("mod:foo",),
            "call_target": "println",
        }
    )
    assert isinstance(payload, RustFactPayloadV1)
    assert payload.call_target == "println"
