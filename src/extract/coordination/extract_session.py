"""Extract session factory helpers."""

from __future__ import annotations

from dataclasses import dataclass

from extract.session import ExtractSession
from extraction.engine_session import EngineSession


@dataclass(frozen=True)
class ExtractSessionRequest:
    """Inputs required to construct an ExtractSession."""

    engine_session: EngineSession


def build_extract_session(request: ExtractSessionRequest) -> ExtractSession:
    """Build an ExtractSession from a normalized request payload.

    Returns:
        ExtractSession: Session wrapper around the provided engine session.
    """
    return ExtractSession(engine_session=request.engine_session)


__all__ = ["ExtractSessionRequest", "build_extract_session"]
