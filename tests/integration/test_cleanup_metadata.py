"""Integration tests for Delta checkpoint + metadata cleanup workflows."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from storage.deltalake.delta import cleanup_delta_log, create_delta_checkpoint
from tests.harness.profiles import conformance_profile_with_sink
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
def test_checkpoint_and_cleanup_metadata_contract(tmp_path: Path) -> None:
    """Assert checkpoint/cleanup calls return structured diagnostics payloads."""
    profile, _ = conformance_profile_with_sink()
    ctx = profile.session_context()
    delta_path = tmp_path / "events_delta"
    source = register_arrow_table(
        ctx,
        name="events_source",
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=source,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    checkpoint_report = create_delta_checkpoint(
        str(delta_path),
        runtime_profile=profile,
        dataset_name="events_delta",
    )
    assert isinstance(checkpoint_report, dict)
    assert checkpoint_report
    cleanup_report = cleanup_delta_log(
        str(delta_path),
        runtime_profile=profile,
        dataset_name="events_delta",
    )
    assert isinstance(cleanup_report, dict)
    assert cleanup_report
