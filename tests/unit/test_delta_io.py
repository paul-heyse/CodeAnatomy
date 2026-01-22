"""Unit tests for Delta IO helpers."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pyarrow as pa

from datafusion_engine.registry_bridge import (
    DeltaCdfRegistrationOptions,
    register_delta_cdf_df,
)
from datafusion_engine.runtime import DataFusionRuntimeProfile, read_delta_table_from_path
from storage.deltalake import (
    DeltaCdfOptions,
    DeltaWriteOptions,
    delta_commit_metadata,
    delta_table_features,
    delta_table_version,
    enable_delta_features,
    write_table_delta,
)


def _sample_table() -> pa.Table:
    return pa.table({"id": [1, 2], "value": ["a", "b"]})


def _read_delta_cdf(path: str, options: DeltaCdfOptions) -> pa.Table:
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    table_name = f"cdf_{uuid4().hex}"
    df = register_delta_cdf_df(
        ctx,
        name=table_name,
        path=path,
        options=DeltaCdfRegistrationOptions(
            cdf_options=options,
            runtime_profile=profile,
        ),
    )
    try:
        return df.to_arrow_table()
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)


def test_delta_write_read_version(tmp_path: Path) -> None:
    """Write a Delta table and read it back."""
    path = tmp_path / "tbl"
    table = _sample_table()
    result = write_table_delta(
        table,
        str(path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    assert result.version == 0
    read_back = read_delta_table_from_path(str(path))
    assert int(read_back.num_rows) == int(table.num_rows)


def test_delta_commit_metadata(tmp_path: Path) -> None:
    """Persist custom commit metadata for Delta writes."""
    path = tmp_path / "metadata"
    write_table_delta(
        _sample_table(),
        str(path),
        options=DeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            commit_metadata={"foo": "bar"},
        ),
    )
    metadata = delta_commit_metadata(str(path))
    assert metadata is not None
    assert metadata.get("foo") == "bar"


def test_delta_cdf_and_features(tmp_path: Path) -> None:
    """Read Delta change data feed and feature flags."""
    path = tmp_path / "cdf"
    write_table_delta(
        _sample_table(),
        str(path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    enable_delta_features(str(path))
    write_table_delta(
        pa.table({"id": [3], "value": ["c"]}),
        str(path),
        options=DeltaWriteOptions(mode="append"),
    )
    version = delta_table_version(str(path))
    assert version == 1
    cdf = _read_delta_cdf(
        str(path),
        options=DeltaCdfOptions(
            starting_version=1,
            ending_version=1,
            allow_out_of_range=True,
        ),
    )
    assert "_change_type" in cdf.schema.names
    assert int(cdf.num_rows) > 0
    features = delta_table_features(str(path))
    assert features is not None
    assert features.get("delta.enableChangeDataFeed") == "true"
