"""Tests for Rust extraction session bridge helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from extraction import rust_session_bridge

if TYPE_CHECKING:
    from datafusion import SessionContext


class _Ctx:
    pass


def test_build_extraction_session_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Session bridge should delegate session construction to extension layer."""
    monkeypatch.setattr(
        rust_session_bridge.datafusion_ext, "build_extraction_session", lambda _p: _Ctx()
    )
    monkeypatch.setattr(rust_session_bridge, "SessionContext", _Ctx)
    assert isinstance(rust_session_bridge.build_extraction_session({}), _Ctx)


def test_register_dataset_provider_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Session bridge should delegate provider registration to extension layer."""
    monkeypatch.setattr(
        rust_session_bridge.datafusion_ext,
        "register_dataset_provider",
        lambda _ctx, _payload: {"table_name": "t"},
    )
    assert rust_session_bridge.register_dataset_provider(
        cast("SessionContext", object()),
        {"table_name": "t"},
    ) == {"table_name": "t"}
