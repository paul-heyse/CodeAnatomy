"""Unit tests for zero-row bootstrap planning and execution."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from datafusion_engine.bootstrap import zero_row as zero_row_bootstrap
from datafusion_engine.bootstrap.zero_row import (
    ZeroRowBootstrapRequest,
    ZeroRowDatasetPlan,
    build_zero_row_plan,
    run_zero_row_bootstrap_validation,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.facade import DataFusionExecutionFacade
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


def test_zero_row_bootstrap_seeded_mode_is_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seeded mode should only seed explicitly configured datasets."""
    plan = (
        ZeroRowDatasetPlan(
            name="seeded_ds",
            role="semantic_input",
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
            location=DatasetLocation(path=str(tmp_path / "seeded_ds"), format="delta"),
            required=True,
        ),
        ZeroRowDatasetPlan(
            name="plain_ds",
            role="semantic_input",
            schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
            location=DatasetLocation(path=str(tmp_path / "plain_ds"), format="delta"),
            required=True,
        ),
    )

    def _build_plan_override(
        _profile: DataFusionRuntimeProfile,
        *,
        request: ZeroRowBootstrapRequest,
    ) -> tuple[ZeroRowDatasetPlan, ...]:
        _ = request
        return plan

    monkeypatch.setattr(
        zero_row_bootstrap,
        "build_zero_row_plan",
        _build_plan_override,
    )
    monkeypatch.setattr(
        zero_row_bootstrap,
        "_validation_errors",
        lambda **_kwargs: [],
    )
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()

    strict_report = run_zero_row_bootstrap_validation(
        profile,
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=False,
            include_internal_tables=False,
            strict=True,
            bootstrap_mode="strict_zero_rows",
            seeded_datasets=("seeded_ds",),
        ),
        ctx=ctx,
    )
    assert strict_report.success
    assert strict_report.seeded_count == 0
    assert strict_report.seeded_datasets == ()
    strict_seeded_count = (
        ctx.sql("SELECT COUNT(*) AS n FROM seeded_ds").to_arrow_table().to_pylist()
    )
    strict_plain_count = ctx.sql("SELECT COUNT(*) AS n FROM plain_ds").to_arrow_table().to_pylist()
    assert strict_seeded_count[0]["n"] == 0
    assert strict_plain_count[0]["n"] == 0

    seeded_report = run_zero_row_bootstrap_validation(
        profile,
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=False,
            include_internal_tables=False,
            strict=True,
            bootstrap_mode="seeded_minimal_rows",
            seeded_datasets=("seeded_ds",),
        ),
        ctx=ctx,
    )
    assert seeded_report.success
    assert seeded_report.seeded_count == 1
    assert seeded_report.seeded_datasets == ("seeded_ds",)
    seeded_count = ctx.sql("SELECT COUNT(*) AS n FROM seeded_ds").to_arrow_table().to_pylist()
    plain_count = ctx.sql("SELECT COUNT(*) AS n FROM plain_ds").to_arrow_table().to_pylist()
    assert seeded_count[0]["n"] == 1
    assert plain_count[0]["n"] == 0


def test_facade_run_zero_row_bootstrap_validation_uses_runtime_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Facade should expose zero-row bootstrap using its bound profile/context."""
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
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    report = facade.run_zero_row_bootstrap_validation(
        request=ZeroRowBootstrapRequest(
            include_semantic_outputs=False,
            include_internal_tables=False,
            strict=True,
        )
    )
    assert report.success
    assert report.failed_count == 0
    assert report.registered_count > 0
    assert ctx.table_exist("cst_refs")


def test_facade_run_zero_row_bootstrap_validation_requires_runtime_profile() -> None:
    """Facade bootstrap should fail fast without a runtime profile."""
    profile = DataFusionRuntimeProfile()
    facade = DataFusionExecutionFacade(ctx=profile.session_context(), runtime_profile=None)
    with pytest.raises(ValueError, match="Runtime profile is required"):
        _ = facade.run_zero_row_bootstrap_validation()
