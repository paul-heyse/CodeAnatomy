"""Tests for UUID temporal helper consolidation."""

from __future__ import annotations

import uuid

from tools.cq.utils.uuid_factory import uuid7, uuid7_time_ms
from tools.cq.utils.uuid_temporal_contracts import uuid_time_millis


def test_uuid_time_millis_matches_uuid7_time_ms_for_v7() -> None:
    value = uuid7()
    assert value.version == 7
    assert uuid_time_millis(value) == uuid7_time_ms(value)
    assert uuid_time_millis(value) is not None


def test_uuid_time_millis_returns_none_for_non_v7() -> None:
    value = uuid.uuid4()
    assert value.version == 4
    assert uuid7_time_ms(value) is None
    assert uuid_time_millis(value) is None
