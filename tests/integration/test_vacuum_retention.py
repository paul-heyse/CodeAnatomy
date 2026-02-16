"""Integration tests for Delta vacuum retention behavior."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.io.write_core import WriteFormat, WriteMode, WritePipeline, WriteRequest
from storage.deltalake.delta_maintenance import vacuum_delta
from storage.deltalake.delta_read import DeltaVacuumOptions
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


@pytest.mark.integration
def test_vacuum_delta_dry_run_contract(tmp_path: Path) -> None:
    """Assert vacuum dry-run contract returns deterministic list payloads."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "events_delta"
    seed = register_arrow_table(
        ctx,
        name="events_seed",
        value=pa.table({"id": [1, 2], "value": ["a", "b"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=seed,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    updates = register_arrow_table(
        ctx,
        name="events_updates",
        value=pa.table({"id": [3], "value": ["c"]}),
    )
    pipeline.write(
        WriteRequest(
            source=updates,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
        )
    )
    dry_run_files = vacuum_delta(
        str(delta_path),
        options=DeltaVacuumOptions(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
        ),
    )
    assert isinstance(dry_run_files, list)
    assert all(isinstance(path, str) for path in dry_run_files)
