"""Integration tests for Ballista distributed contexts."""

from __future__ import annotations

import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile

ballista = pytest.importorskip("ballista")
pytest.importorskip("datafusion")


@pytest.mark.integration
def test_ballista_context_factory_smoke() -> None:
    """Construct a Ballista-backed SessionContext."""
    _ = ballista, DataFusionRuntimeProfile
    pytest.skip("Distributed SessionContext factories were removed in design-phase refactor.")
