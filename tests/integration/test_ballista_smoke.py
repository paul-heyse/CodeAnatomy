"""Integration tests for Ballista distributed contexts."""

from __future__ import annotations

import pytest

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.optional_deps import require_datafusion_udfs

ballista = pytest.importorskip("ballista")
require_datafusion_udfs()


@pytest.mark.integration
def test_ballista_context_factory_smoke() -> None:
    """Construct a Ballista-backed SessionContext."""
    _ = ballista, DataFusionRuntimeProfile
    pytest.skip("Distributed SessionContext factories were removed in design-phase refactor.")
