"""Integration tests for resolve_dataset_provider boundary conditions.

Tests Suite 3.3: Provider resolution choke-point with 7 callsites across 7 files.
Target: src/datafusion_engine/dataset/resolution.py:74
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.dataset.resolution import (
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from datafusion_engine.errors import DataFusionEngineError, ErrorKind
from datafusion_engine.io.write_core import WriteFormat, WriteMode, WritePipeline, WriteRequest
from storage.deltalake import DeltaCdfOptions
from tests.harness.profiles import conformance_profile
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


def _write_delta_table(ctx: SessionContext, delta_path: Path) -> int:
    """Write a simple Delta table for testing.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context
    delta_path : Path
        Path where Delta table will be written

    Returns:
    -------
    int
        Version number of written Delta table
    """
    profile = conformance_profile()
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


def _write_delta_table_with_cdf(delta_path: Path) -> int:
    """Write a Delta table with CDF enabled.

    Parameters
    ----------
    delta_path : Path
        Path where Delta table will be written

    Returns:
    -------
    int
        Version number of written Delta table
    """
    from deltalake import write_deltalake

    table = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    write_deltalake(
        str(delta_path),
        table,
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    return 0


@pytest.mark.integration
def test_resolve_delta_provider_roundtrip(tmp_path: Path) -> None:
    """Write Delta table, resolve as provider_kind=delta, verify resolution."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "test_delta"

    _write_delta_table(ctx, delta_path)

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
    )

    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.provider_kind == "delta"
    assert resolution.provider is not None
    assert resolution.delta_snapshot is not None
    assert isinstance(resolution.delta_scan_identity_hash, str)
    assert len(resolution.delta_scan_identity_hash) > 0
    assert resolution.location == location
    assert resolution.cdf_options is None


@pytest.mark.integration
def test_resolve_delta_cdf_provider(tmp_path: Path) -> None:
    """Write Delta table with CDF enabled, resolve as provider_kind=delta_cdf."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "test_delta_cdf"

    _write_delta_table_with_cdf(delta_path)

    cdf_options = DeltaCdfOptions(starting_version=0)
    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
        delta_cdf_options=cdf_options,
    )

    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.provider_kind == "delta_cdf"
    assert resolution.provider is not None
    assert resolution.cdf_options is not None


@pytest.mark.integration
def test_resolution_captures_snapshot_key(tmp_path: Path) -> None:
    """Resolve provider, verify snapshot metadata is captured."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "test_snapshot"

    _write_delta_table(ctx, delta_path)

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
    )

    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.delta_scan_identity_hash is not None
    assert isinstance(resolution.delta_scan_identity_hash, str)
    assert len(resolution.delta_scan_identity_hash) > 0
    assert resolution.delta_snapshot is not None
    assert isinstance(resolution.delta_snapshot, dict)


@pytest.mark.integration
def test_scan_identity_hash_deterministic(tmp_path: Path) -> None:
    """Resolve same table twice, verify hash stability."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "test_deterministic"

    _write_delta_table(ctx, delta_path)

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
    )

    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
    )

    resolution1 = resolve_dataset_provider(request)
    resolution2 = resolve_dataset_provider(request)

    assert resolution1.delta_scan_identity_hash == resolution2.delta_scan_identity_hash
    assert resolution1.delta_scan_identity_hash is not None


@pytest.mark.integration
def test_invalid_predicate_captured(tmp_path: Path) -> None:
    """Pass invalid SQL predicate, verify predicate_error is captured."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "test_predicate"

    _write_delta_table(ctx, delta_path)

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
    )

    invalid_predicate = "not_a_column > 5"
    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
        predicate=invalid_predicate,
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.predicate_error is not None
    assert isinstance(resolution.predicate_error, str)
    assert len(resolution.predicate_error) > 0


@pytest.mark.integration
def test_control_plane_failure_wraps_error(tmp_path: Path) -> None:
    """Corrupt Delta table location, attempt resolution, expect DataFusionEngineError."""
    profile = conformance_profile()
    ctx = profile.session_context()
    corrupt_path = tmp_path / "nonexistent_table"

    location = DatasetLocation(
        path=str(corrupt_path),
        format="delta",
    )

    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
    )

    with pytest.raises(DataFusionEngineError) as exc_info:
        resolve_dataset_provider(request)

    assert exc_info.value.kind == ErrorKind.PLUGIN


@pytest.mark.integration
def test_scan_files_override(tmp_path: Path) -> None:
    """Pass explicit scan_files list, verify provider uses file pruning."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "test_scan_files"

    _write_delta_table(ctx, delta_path)

    from deltalake import DeltaTable

    dt = DeltaTable(str(delta_path))
    add_actions = dt.get_add_actions()
    if add_actions is None:
        pytest.skip("No add actions found in Delta table")

    add_actions_table = pa.table(add_actions)
    if add_actions_table.num_rows == 0:
        pytest.skip("No add actions found in Delta table")

    first_file = add_actions_table.to_pylist()[0]["path"]

    location = DatasetLocation(
        path=str(delta_path),
        format="delta",
    )

    request = DatasetResolutionRequest(
        ctx=ctx,
        location=location,
        runtime_profile=profile,
        scan_files=[first_file],
    )

    resolution = resolve_dataset_provider(request)

    assert resolution.provider is not None
    assert resolution.add_actions is not None
    assert len(resolution.add_actions) > 0


__all__ = [
    "test_control_plane_failure_wraps_error",
    "test_invalid_predicate_captured",
    "test_resolution_captures_snapshot_key",
    "test_resolve_delta_cdf_provider",
    "test_resolve_delta_provider_roundtrip",
    "test_scan_files_override",
    "test_scan_identity_hash_deterministic",
]
