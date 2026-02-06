"""Integration smoke tests for active runtime context construction."""

from __future__ import annotations

import pytest

from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()


@pytest.mark.integration
def test_runtime_context_factory_smoke() -> None:
    """Construct runtime context and verify extension capability visibility."""
    profile = DataFusionRuntimeProfile()
    runtime = profile.session_runtime()
    assert runtime.ctx is profile.session_context()
    compatibility = is_delta_extension_compatible(runtime.ctx)
    assert compatibility.available is True
    assert compatibility.compatible is True
    assert compatibility.entrypoint == "delta_scan_config_from_session"
    assert compatibility.module is not None
    assert compatibility.ctx_kind is not None
    assert compatibility.probe_result == "ok"
