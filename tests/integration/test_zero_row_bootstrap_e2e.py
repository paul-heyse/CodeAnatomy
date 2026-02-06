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
    assert (
        artifacts_root / "datafusion_view_cache_inventory_v1" / "_delta_log"
    ).exists()
