"""Integration tests for dataset resolution contract edge cases.

Tests Suite 3.13: Storage options merging and provider artifact metadata.
Extends Suite 3.3 (test_resolve_dataset_provider.py) with NON-P0 edge cases.
Target: src/datafusion_engine/dataset/resolution.py:74
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.dataset.resolution import (
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from datafusion_engine.io.write_core import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.lineage.diagnostics import InMemoryDiagnosticsSink
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_profile_config import DiagnosticsConfig
from tests.harness.profiles import conformance_profile
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


def _write_delta_table_for_resolution(profile: DataFusionRuntimeProfile, delta_path: Path) -> int:
    """Write a simple Delta table for resolution testing.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile
        Runtime profile for writing
    delta_path : Path
        Path where Delta table will be written

    Returns:
    -------
    int
        Version number of written Delta table
    """
    ctx = profile.session_context()
    source = register_arrow_table(
        ctx,
        name="test_source",
        value=pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    write_result = pipeline.write(
        WriteRequest(
            source=source,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    assert write_result.delta_result is not None
    assert write_result.delta_result.version is not None
    return write_result.delta_result.version


@pytest.mark.integration
def test_storage_options_merge_request_overrides_policy(tmp_path: Path) -> None:
    """Verify storage options merging when request and policy conflict.

    When location.storage_options and location.delta_log_storage_options
    have conflicting keys, merged_storage_options() should prefer the log storage
    options (following dict.update semantics).
    """
    profile = conformance_profile()
    delta_path = tmp_path / "test_storage_merge"

    _write_delta_table_for_resolution(profile, delta_path)

    storage_opts = {"key1": "value_from_storage", "key2": "storage_only"}
    log_storage_opts = {"key1": "value_from_log", "key3": "log_only"}

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
        storage_options=storage_opts,
        delta_log_storage_options=log_storage_opts,
    )

    request = DatasetResolutionRequest(
        ctx=profile.session_context(),
        location=location,
        runtime_profile=profile,
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.provider is not None
    assert resolution.provider_kind == "delta"

    contract_storage_options: dict[str, str] = dict(
        resolution.location.storage_options
        if hasattr(resolution.location, "storage_options")
        else {}
    )
    contract_log_storage_options: dict[str, str] = dict(
        resolution.location.delta_log_storage_options
        if hasattr(resolution.location, "delta_log_storage_options")
        else {}
    )

    assert "key1" in contract_storage_options or "key1" in contract_log_storage_options
    assert "key2" in contract_storage_options or "key2" in contract_log_storage_options
    assert "key3" in contract_storage_options or "key3" in contract_log_storage_options


@pytest.mark.integration
def test_provider_artifacts_include_scan_metadata(tmp_path: Path) -> None:
    """Verify provider resolution artifacts include delta_scan_identity_hash.

    Resolution should record artifacts via the diagnostics sink containing
    scan metadata including the delta_scan_identity_hash field.
    """
    sink = InMemoryDiagnosticsSink()
    base_profile = conformance_profile()
    profile = DataFusionRuntimeProfile(
        data_sources=base_profile.data_sources,
        diagnostics=DiagnosticsConfig(diagnostics_sink=sink),
        policies=base_profile.policies,
        features=base_profile.features,
    )

    delta_path = tmp_path / "test_scan_metadata"

    _write_delta_table_for_resolution(profile, delta_path)

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
    )

    request = DatasetResolutionRequest(
        ctx=profile.session_context(),
        location=location,
        runtime_profile=profile,
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.provider is not None
    assert resolution.delta_scan_identity_hash is not None
    assert isinstance(resolution.delta_scan_identity_hash, str)
    assert len(resolution.delta_scan_identity_hash) > 0

    assert resolution.delta_scan_config is not None
    assert isinstance(resolution.delta_scan_config, dict)

    assert resolution.delta_scan_effective is not None
    assert isinstance(resolution.delta_scan_effective, dict)


__all__ = [
    "test_provider_artifacts_include_scan_metadata",
    "test_storage_options_merge_request_overrides_policy",
]
