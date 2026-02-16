"""Tests for ripgrep JSON codec helpers."""

from __future__ import annotations

import msgspec
import pytest
from tools.cq.search.rg.codec import (
    as_begin_data,
    as_context_data,
    as_end_data,
    decode_event,
    decode_event_strict,
    decode_rg_event,
)


def test_decode_event_returns_mapping() -> None:
    event = decode_event(b'{"type":"summary","data":{"stats":{"matches":1}}}')
    assert isinstance(event, dict)
    assert event.get("type") == "summary"


def test_decode_event_returns_none_on_invalid_json() -> None:
    assert decode_event(b"{oops") is None


def test_decode_event_strict_raises_on_invalid_json() -> None:
    with pytest.raises((msgspec.DecodeError, msgspec.ValidationError)):
        decode_event_strict(b"{oops")


def test_decode_context_begin_end_events() -> None:
    context = decode_rg_event(
        b'{"type":"context","data":{"path":{"text":"a.py"},"lines":{"text":"x"},"line_number":4}}'
    )
    begin = decode_rg_event(b'{"type":"begin","data":{"path":{"text":"a.py"}}}')
    end = decode_rg_event(b'{"type":"end","data":{"path":{"text":"a.py"},"binary_offset":8}}')

    assert context is not None
    assert begin is not None
    assert end is not None
    assert as_context_data(context) is not None
    assert as_begin_data(begin) is not None
    end_data = as_end_data(end)
    assert end_data is not None
    assert end_data.binary_offset == 8
