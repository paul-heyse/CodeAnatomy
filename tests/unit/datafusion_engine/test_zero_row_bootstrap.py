"""Unit tests for zero-row bootstrap planning and execution."""

from __future__ import annotations

from pathlib import Path

import pytest

from datafusion_engine.bootstrap import zero_row as zero_row_bootstrap
from datafusion_engine.bootstrap.zero_row import (
    ZeroRowBootstrapRequest,
    build_zero_row_plan,
    run_zero_row_bootstrap_validation,
)
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    ExtractOutputConfig,
    SemanticOutputConfig,
)
from tests.test_helpers.optional_deps import require_datafusion_udfs, require_deltalake

require_datafusion_udfs()
require_deltalake()


def test_build_zero_row_plan_includes_semantic_inputs_and_canonical_outputs(
    tmp_path: Path,
) -> None:
    """Plan should include semantic input sources and canonical semantic outputs."""
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            extract_output=ExtractOutputConfig(output_root=str(tmp_path / "extract")),
            semantic_output=SemanticOutputConfig(output_root=str(tmp_path / "semantic")),
        ),
    )
    plan = build_zero_row_plan(
        profile,
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=True,
            include_internal_tables=False,
            strict=True,
        ),
    )
    by_name = {entry.name: entry for entry in plan}
    assert "cst_refs" in by_name
    assert by_name["cst_refs"].role == "semantic_input"
    assert by_name["cst_refs"].required is True
    assert "cpg_nodes" in by_name
    assert by_name["cpg_nodes"].role == "semantic_output"
    assert "cpg_nodes_v1" not in by_name


def test_zero_row_bootstrap_execution_registers_required_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap execution should materialize and register required semantic inputs."""
    monkeypatch.setattr(
        zero_row_bootstrap,
        "extract_nested_dataset_names",
        lambda: (),
    )
    monkeypatch.setattr(
        zero_row_bootstrap,
        "relationship_schema_names",
        lambda: (),
    )
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            extract_output=ExtractOutputConfig(output_root=str(tmp_path / "extract")),
        ),
    )
    ctx = profile.session_context()
    report = run_zero_row_bootstrap_validation(
        profile,
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=False,
            include_internal_tables=False,
            strict=True,
        ),
        ctx=ctx,
    )
    assert report.success
    assert report.failed_count == 0
    assert report.registered_count > 0
    cst_refs_path = Path(tmp_path / "extract" / "cst_refs")
    assert (cst_refs_path / "_delta_log").exists()
    assert ctx.table_exist("cst_refs")
