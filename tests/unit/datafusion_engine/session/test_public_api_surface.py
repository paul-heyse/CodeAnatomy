# ruff: noqa: D100, D103
from __future__ import annotations

from datafusion_engine import session


def test_session_public_api_surface_is_explicit() -> None:
    assert session.__all__ == [
        "DataFusionExecutionFacade",
        "DataFusionRuntimeProfile",
        "RuntimeProfileConfig",
    ]
