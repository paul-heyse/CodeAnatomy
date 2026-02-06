"""Integration tests for Delta snapshot identity contracts."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from tests.harness.profiles import conformance_profile
from tests.harness.snapshot_key import assert_snapshot_key_payload, snapshot_key_payload
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
def test_snapshot_key_payload_is_deterministic(tmp_path: Path) -> None:
    """Assert local Delta writes produce canonical snapshot identity payloads."""
    profile = conformance_profile()
    ctx = profile.session_context()
    delta_path = tmp_path / "events_delta"
    source = register_arrow_table(
        ctx,
        name="events_source",
        value=pa.table({"id": [1, 2, 3]}),
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
    payload = snapshot_key_payload(str(delta_path), write_result.delta_result.version)
    assert_snapshot_key_payload(
        payload,
        table_uri=str(delta_path),
        version=write_result.delta_result.version,
    )
    assert write_result.delta_result.snapshot_key is not None
    assert write_result.delta_result.snapshot_key.canonical_uri == payload["canonical_uri"]
    assert write_result.delta_result.snapshot_key.version == payload["resolved_version"]


@pytest.mark.integration
def test_snapshot_key_payload_normalizes_s3_aliases() -> None:
    """Assert snapshot key canonicalizes s3a/s3n aliases to s3."""
    s3a_payload = snapshot_key_payload("s3a://Example-Bucket/path/table", 11)
    s3n_payload = snapshot_key_payload("s3n://example-bucket/path/table", 11)
    assert s3a_payload == s3n_payload
    assert s3a_payload["canonical_uri"] == "s3://example-bucket/path/table"
