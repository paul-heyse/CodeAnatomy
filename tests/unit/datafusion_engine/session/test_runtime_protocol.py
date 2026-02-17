"""Unit tests for runtime protocol typing surface."""

from __future__ import annotations

from typing import cast

from datafusion import SessionContext, SQLOptions

from datafusion_engine.session.runtime_protocol import RuntimeProfileLike


class _RuntimeProfileImpl:
    def __init__(self) -> None:
        from datafusion import SessionContext

        self._ctx = SessionContext()

    def __getattr__(self, _name: str) -> object:
        return object()

    def session_context(self) -> SessionContext:
        return self._ctx

    def sql_options(self) -> SQLOptions:
        _ = self
        return SQLOptions()


def test_runtime_profile_like_protocol() -> None:
    """Validate runtime-profile protocol compatibility for mixin consumers."""
    profile = cast("RuntimeProfileLike", _RuntimeProfileImpl())
    assert profile.session_context() is not None
    assert isinstance(profile.sql_options(), SQLOptions)
