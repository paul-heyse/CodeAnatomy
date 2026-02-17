"""Tests for explicit public API surface in session package."""

from __future__ import annotations

from datafusion_engine import session


def test_session_public_api_surface_is_explicit() -> None:
    """Session package exports stable explicit API list."""
    assert session.__all__ == [
        "DataFusionExecutionFacade",
        "DataFusionRuntimeProfile",
        "RuntimeProfileConfig",
    ]
