"""Tests for extract-session builder helper."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from extract.coordination.extract_session import ExtractSessionRequest, build_extract_session

if TYPE_CHECKING:
    from extraction.engine_session import EngineSession


class _EngineSession:
    pass


def test_build_extract_session() -> None:
    """Extract-session helper should construct session from request payload."""
    session = build_extract_session(
        ExtractSessionRequest(engine_session=cast("EngineSession", _EngineSession()))
    )
    assert session.engine_session is not None
