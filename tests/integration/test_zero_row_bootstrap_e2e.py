"""End-to-end integration tests for zero-row bootstrap validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from datafusion_engine.bootstrap.zero_row import ZeroRowBootstrapRequest
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    ExtractOutputConfig,
    PolicyBundleConfig,
    SemanticOutputConfig,
)
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


@pytest.mark.integration
def test_zero_row_bootstrap_validation_e2e(tmp_path: Path) -> None:
    """Bootstrap should create zero-row Delta datasets and validate runtime inputs."""
    extract_root = tmp_path / "extract"
    semantic_root = tmp_path / "semantic"
    artifacts_root = tmp_path / "artifacts"
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            extract_output=ExtractOutputConfig(output_root=str(extract_root)),
            semantic_output=SemanticOutputConfig(output_root=str(semantic_root)),
        ),
        policies=PolicyBundleConfig(plan_artifacts_root=str(artifacts_root)),
    )

    report = profile.run_zero_row_bootstrap_validation(
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=True,
            include_internal_tables=True,
            strict=True,
        )
    )

    assert report.success
    assert report.failed_count == 0
    assert report.validation_errors == ()
    assert report.registered_count > 0

    ctx = profile.session_context()
    assert ctx.table_exist("cst_refs")
    assert ctx.table_exist("cpg_nodes")
    assert (extract_root / "cst_refs" / "_delta_log").exists()
    assert (semantic_root / "cpg_nodes" / "_delta_log").exists()
    assert (artifacts_root / "datafusion_view_cache_inventory_v1" / "_delta_log").exists()


@pytest.mark.integration
def test_zero_row_bootstrap_seeded_mode_only_seeds_selected_datasets(tmp_path: Path) -> None:
    """Seeded mode should only materialize seed rows for selected datasets."""
    extract_root = tmp_path / "extract"
    semantic_root = tmp_path / "semantic"
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            extract_output=ExtractOutputConfig(output_root=str(extract_root)),
            semantic_output=SemanticOutputConfig(output_root=str(semantic_root)),
        ),
    )

    report = profile.run_zero_row_bootstrap_validation(
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=False,
            include_internal_tables=False,
            strict=True,
            bootstrap_mode="seeded_minimal_rows",
            seeded_datasets=("cst_refs",),
        )
    )

    assert report.success
    assert report.seeded_count == 1
    assert report.seeded_datasets == ("cst_refs",)

    ctx = profile.session_context()
    seeded_count = ctx.sql("SELECT COUNT(*) AS n FROM cst_refs").to_arrow_table().to_pylist()
    plain_count = ctx.sql("SELECT COUNT(*) AS n FROM cst_defs").to_arrow_table().to_pylist()
    assert seeded_count[0]["n"] == 1
    assert plain_count[0]["n"] == 0
