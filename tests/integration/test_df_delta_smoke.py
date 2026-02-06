"""Delta/DataFusion conformance smoke tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.harness.delta_smoke import DeltaSmokeScenario, run_delta_smoke_round_trip
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


@pytest.mark.integration
def test_df_delta_smoke_round_trip(tmp_path: Path) -> None:
    """Write a Delta table and query it through runtime-session provider registration."""
    result = run_delta_smoke_round_trip(DeltaSmokeScenario(tmp_path=tmp_path))
    assert result.delta_version >= 0
    assert result.manifest_path.exists()
    assert result.details_path.exists()
    assert list(result.rows) == [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]
    assert result.runtime_capabilities
    assert (
        result.runtime_capabilities[-1].get("strict_native_provider_enabled")
        is result.strict_native_provider_enabled
    )
    assert result.provider_artifacts
    last = result.provider_artifacts[-1]
    assert last.get("strict_native_provider_enabled") is result.strict_native_provider_enabled
    if result.strict_native_provider_enabled:
        assert last.get("provider_mode") == "delta_table_provider"
        assert last.get("strict_native_provider_violation") is False
        assert result.service_provider_artifacts
        service_last = result.service_provider_artifacts[-1]
        assert service_last.get("provider_kind") == "delta"
        assert service_last.get("strict_native_provider_enabled") is True
        snapshot_key = service_last.get("snapshot_key")
        assert isinstance(snapshot_key, dict)
        assert snapshot_key.get("resolved_version") == result.delta_version
        assert isinstance(snapshot_key.get("canonical_uri"), str)
        fingerprint = service_last.get("storage_profile_fingerprint")
        assert isinstance(fingerprint, str)
        assert fingerprint
    elif result.service_provider_artifacts:
        service_last = result.service_provider_artifacts[-1]
        assert service_last.get("strict_native_provider_enabled") is False
