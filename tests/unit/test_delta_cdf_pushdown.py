"""Tests for Delta CDF pushdown behavior."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.delta_seed import DeltaSeedOptions, write_delta_table
from tests.test_helpers.optional_deps import require_datafusion, require_deltalake

require_datafusion()
pytest.importorskip("datafusion_ext")
require_deltalake()


def _create_cdf_table(path: Path) -> None:
    from storage.deltalake.delta import DeltaFeatureMutationOptions, enable_delta_features

    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    try:
        _ = write_delta_table(
            path.parent,
            table=table,
            options=DeltaSeedOptions(
                profile=df_profile(),
                table_name=path.name,
            ),
        )
        enable_delta_features(DeltaFeatureMutationOptions(path=str(path)))
    except RuntimeError as exc:
        pytest.skip(str(exc))


def test_delta_cdf_projection_and_filter_pushdown(tmp_path: Path) -> None:
    """Ensure CDF providers honor projection and filter pushdown."""
    from datafusion_engine.dataset.registration import register_dataset_df
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.datafusion import extract_lineage
    from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle
    from storage.deltalake import DeltaCdfOptions

    table_path = tmp_path / "cdf_table"
    _create_cdf_table(table_path)
    runtime = df_profile()
    ctx = runtime.session_context()
    register_dataset_df(
        ctx,
        name="cdf_table",
        location=DatasetLocation(
            path=table_path,
            format="delta",
            delta_cdf_options=DeltaCdfOptions(starting_version=0),
        ),
        runtime_profile=runtime,
    )
    df = ctx.sql("SELECT id FROM cdf_table WHERE id > 1")
    bundle = build_plan_bundle(
        ctx,
        df,
        options=PlanBundleOptions(
            session_runtime=runtime.session_runtime(),
        ),
    )
    lineage = extract_lineage(bundle.optimized_logical_plan)
    scan = next(
        (scan for scan in lineage.scans if scan.dataset_name == "cdf_table"),
        None,
    )
    assert scan is not None
    assert "id" in scan.projected_columns
    assert "value" not in scan.projected_columns
    assert any("id" in expr and ">" in expr for expr in scan.pushed_filters)
