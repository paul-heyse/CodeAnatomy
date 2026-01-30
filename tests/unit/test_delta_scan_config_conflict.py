"""Tests for Delta scan configuration validation."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.delta_seed import DeltaSeedOptions, write_delta_table
from tests.test_helpers.optional_deps import require_datafusion, require_deltalake

require_datafusion()
require_deltalake()


def test_delta_file_column_conflict_raises(tmp_path: Path) -> None:
    """Raise when Delta file column collides with data columns."""
    from datafusion_engine.dataset.registration import register_dataset_df
    from datafusion_engine.dataset.registry import DatasetLocation

    table = pa.table({"__delta_rs_path": ["a", "b"], "value": [1, 2]})
    profile, ctx, delta_path = write_delta_table(
        tmp_path,
        table=table,
        options=DeltaSeedOptions(
            profile=df_profile(),
            table_name="delta_table",
        ),
    )
    with pytest.raises(RuntimeError):
        register_dataset_df(
            ctx,
            name="delta_tbl",
            location=DatasetLocation(path=str(delta_path), format="delta"),
            runtime_profile=profile,
        )
