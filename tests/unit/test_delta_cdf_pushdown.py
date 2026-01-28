"""Tests for Delta CDF pushdown behavior."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

pytest.importorskip("datafusion")
pytest.importorskip("datafusion_ext")
pytest.importorskip("deltalake")


def _create_cdf_table(path: Path) -> None:
    from storage.deltalake import DeltaWriteOptions, write_delta_table
    from storage.deltalake.delta import DeltaFeatureMutationOptions, enable_delta_features

    table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    try:
        write_delta_table(
            table,
            str(path),
            options=DeltaWriteOptions(mode="overwrite"),
        )
        enable_delta_features(DeltaFeatureMutationOptions(path=str(path)))
    except RuntimeError as exc:
        pytest.skip(str(exc))


def test_delta_cdf_projection_and_filter_pushdown(tmp_path: Path) -> None:
    """Ensure CDF providers honor projection and filter pushdown."""
    from datafusion_engine.lineage_datafusion import extract_lineage
    from datafusion_engine.plan_bundle import build_plan_bundle
    from datafusion_engine.registry_bridge import (
        DeltaCdfRegistrationOptions,
        register_delta_cdf_df,
    )
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from storage.deltalake import DeltaCdfOptions

    table_path = tmp_path / "cdf_table"
    _create_cdf_table(table_path)
    runtime = DataFusionRuntimeProfile()
    ctx = runtime.session_context()
    register_delta_cdf_df(
        ctx,
        name="cdf_table",
        path=str(table_path),
        options=DeltaCdfRegistrationOptions(
            cdf_options=DeltaCdfOptions(starting_version=0),
            runtime_profile=runtime,
        ),
    )
    df = ctx.sql("SELECT id FROM cdf_table WHERE id > 1")
    bundle = build_plan_bundle(
        ctx,
        df,
        session_runtime=runtime.session_runtime(),
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
