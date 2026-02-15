"""Tests for ripgrep JSON codec helpers."""

from __future__ import annotations

import msgspec
import pytest
from tools.cq.search.rg.codec import decode_event, decode_event_strict


def test_decode_event_returns_mapping() -> None:
    event = decode_event(b'{"type":"summary","data":{"stats":{"matches":1}}}')
    assert isinstance(event, dict)
    assert event.get("type") == "summary"


def test_decode_event_returns_none_on_invalid_json() -> None:
    assert decode_event(b"{oops") is None


def test_decode_event_strict_raises_on_invalid_json() -> None:
    with pytest.raises((msgspec.DecodeError, msgspec.ValidationError)):
        decode_event_strict(b"{oops")
