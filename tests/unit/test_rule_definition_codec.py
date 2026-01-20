"""Tests for rule definition codec round-trips."""

from __future__ import annotations

from relspec.rules.cache import rule_definitions_cached
from relspec.rules.spec_codec import rule_definition_codec


def test_rule_definition_codec_round_trip() -> None:
    """Round-trip rule definitions through the codec."""
    rules = rule_definitions_cached("normalize")
    codec = rule_definition_codec()
    table = codec.encode_table(rules)
    decoded = codec.decode_table(table)
    assert decoded == rules
