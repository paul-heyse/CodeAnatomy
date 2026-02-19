"""Tests for canonical extraction entry-point materialization flow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from extract.coordination.context import ExtractExecutionContext
from extract.coordination.entry_point import run_extract_entry_point

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.session import ExtractSession


@dataclass
class _FakeDiagnostics:
    capture_plan_artifacts: bool = False


@dataclass
class _FakeRuntimeProfile:
    diagnostics: _FakeDiagnostics


@dataclass
class _FakeSession:
    runtime_profile: _FakeRuntimeProfile
    determinism_tier: object


def test_run_extract_entry_point_materializes_with_canonical_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entry-point wrapper should preserve canonical setup/materialization flow."""
    recorded: dict[str, object] = {}

    def _plan_builder(
        repo_files: pa.Table,
        options: dict[str, bool],
        exec_context: ExtractExecutionContext,
        session: ExtractSession,
        runtime_profile: DataFusionRuntimeProfile,
    ) -> DataFusionPlanArtifact:
        recorded["repo_files"] = repo_files
        recorded["options"] = options
        recorded["session"] = session
        recorded["runtime_profile"] = runtime_profile
        assert exec_context.session is session
        return cast("DataFusionPlanArtifact", {"plan": "artifact"})

    def _materialize_extract_plan(
        name: str,
        plan: DataFusionPlanArtifact,
        **kwargs: object,
    ) -> dict[str, str]:
        recorded["dataset_name"] = name
        recorded["plan"] = plan
        recorded["materialize_kwargs"] = kwargs
        return {"table": "ok"}

    monkeypatch.setattr(
        "extract.coordination.entry_point.materialize_extract_plan",
        _materialize_extract_plan,
    )

    session = _FakeSession(
        runtime_profile=_FakeRuntimeProfile(_FakeDiagnostics(capture_plan_artifacts=False)),
        determinism_tier="strict",
    )
    context = ExtractExecutionContext(session=cast("ExtractSession", session))

    result = run_extract_entry_point(
        "ast",
        "ast_files_v1",
        repo_files=pa.table({"repo": ["files"]}),
        options={"parallel": True},
        context=context,
        plan_builder=_plan_builder,
    )

    assert result.extractor_name == "ast"
    assert result.table == {"table": "ok"}
    assert recorded["dataset_name"] == "ast_files_v1"
    assert recorded["plan"] == {"plan": "artifact"}
