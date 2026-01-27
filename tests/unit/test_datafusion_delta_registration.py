"""Unit tests for DataFusion Delta registration."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.sources import IbisDeltaWriteOptions
from tests.utils import write_delta_table

datafusion = pytest.importorskip("datafusion")


@pytest.mark.unit
def test_register_delta_dataset(tmp_path: Path) -> None:
    """Register a Delta table into DataFusion and validate scan output."""
    table = pa.table({"id": [1, 2], "value": ["a", "b"]})
    path = tmp_path / "delta_table"
    write_delta_table(
        table,
        str(path),
        options=IbisDeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )

    ctx = DataFusionRuntimeProfile().session_context()
    df = register_dataset_df(
        ctx,
        name="delta_tbl",
        location=DatasetLocation(path=str(path), format="delta"),
    )
    to_arrow = getattr(df, "to_arrow_table", None)
    assert callable(to_arrow)
    result = cast("pa.Table", to_arrow())
    assert int(result.num_rows) == int(table.num_rows)
