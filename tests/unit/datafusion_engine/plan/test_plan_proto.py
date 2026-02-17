"""Tests for plan proto helper bridge."""

from __future__ import annotations

import pytest

from datafusion_engine.plan import plan_proto


def test_plan_to_proto_bytes_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan-to-proto helper should delegate to bundle helper function."""
    monkeypatch.setattr(plan_proto, "_plan_proto_bytes", lambda *_args, **_kwargs: b"proto")
    assert plan_proto.plan_to_proto_bytes(object(), enabled=True) == b"proto"


def test_proto_serialization_enabled_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Feature probe helper should delegate to bundle helper function."""
    monkeypatch.setattr(plan_proto, "_proto_serialization_enabled", lambda: True)
    assert plan_proto.proto_serialization_enabled() is True
