"""Tests for UUID temporal helper consolidation."""

from __future__ import annotations

import uuid

from tools.cq.utils.uuid_factory import uuid7, uuid7_time_ms
from tools.cq.utils.uuid_temporal_contracts import uuid_time_millis

UUID7_VERSION = 7
UUID4_VERSION = 4


def test_uuid_time_millis_matches_uuid7_time_ms_for_v7() -> None:
    """Test uuid time millis matches uuid7 time ms for v7."""
    value = uuid7()
    assert value.version == UUID7_VERSION
    assert uuid_time_millis(value) == uuid7_time_ms(value)
    assert uuid_time_millis(value) is not None


def test_uuid_time_millis_returns_none_for_non_v7() -> None:
    """Test uuid time millis returns none for non v7."""
    value = uuid.uuid4()
    assert value.version == UUID4_VERSION
    assert uuid7_time_ms(value) is None
    assert uuid_time_millis(value) is None
