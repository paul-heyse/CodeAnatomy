"""Tests for Delta scan configuration validation."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("datafusion")
pytest.importorskip("deltalake")


def test_delta_file_column_conflict_raises(tmp_path: Path) -> None:
    """Raise when Delta file column collides with data columns."""
    from datafusion_engine.dataset_registration import register_dataset_df
    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.ingest import datafusion_from_arrow
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from datafusion_engine.write_pipeline import WriteFormat, WriteMode, WritePipeline, WriteRequest

    table = pa.table({"__delta_rs_path": ["a", "b"], "value": [1, 2]})
    delta_path = tmp_path / "delta_table"
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    df = datafusion_from_arrow(ctx, name="delta_seed", value=table)
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=str(delta_path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    with pytest.raises(RuntimeError):
        register_dataset_df(
            ctx,
            name="delta_tbl",
            location=DatasetLocation(path=str(delta_path), format="delta"),
            runtime_profile=profile,
        )
