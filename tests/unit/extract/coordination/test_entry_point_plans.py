"""Tests for canonical extraction plan-only entry-point behavior."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from extract.coordination.context import ExtractExecutionContext
from extract.coordination.entry_point import run_extract_plan_entry_point

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
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


def test_run_extract_plan_entry_point_preserves_plan_keys() -> None:
    """Plan-only entrypoint should preserve output keys and payload mapping."""
    session = _FakeSession(
        runtime_profile=_FakeRuntimeProfile(_FakeDiagnostics(capture_plan_artifacts=False)),
        determinism_tier="strict",
    )
    context = ExtractExecutionContext(session=cast("ExtractSession", session))

    plans = run_extract_plan_entry_point(
        repo_files=pa.table({"repo": ["files"]}),
        options={"parallel": True},
        context=context,
        plan_builder=lambda *_args: cast(
            "dict[str, DataFusionPlanArtifact]",
            {
                "ast_files": {"plan": 1},
                "ast_nodes": {"plan": 2},
            },
        ),
    )

    assert set(plans) == {"ast_files", "ast_nodes"}
    assert plans["ast_files"] == {"plan": 1}
