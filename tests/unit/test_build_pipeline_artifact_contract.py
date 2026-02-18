"""Unit tests for run_build artifact-contract consumption in build_pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from extraction.orchestrator import ExtractionResult
from graph import build_pipeline
from graph.build_pipeline import _AuxiliaryOutputOptions
from planning_engine.spec_contracts import (
    InputRelation,
    JoinGraph,
    OutputTarget,
    SemanticExecutionSpec,
    ViewDefinition,
)


def test_collect_auxiliary_outputs_uses_contract_paths(tmp_path: Path) -> None:
    """Test collect auxiliary outputs uses contract paths."""
    extraction_result = ExtractionResult(
        delta_locations={},
        semantic_input_locations={},
        errors=[{"error": "x"}],
        timing={},
    )
    artifacts: dict[str, object] = {
        "manifest_path": str(tmp_path / "run_manifest"),
        "run_bundle_dir": str(tmp_path / "run_bundle"),
        "auxiliary_outputs": {
            "normalize_outputs_delta": str(tmp_path / "normalize_outputs"),
            "extract_error_artifacts_delta": str(tmp_path / "extract_errors"),
            "run_manifest_delta": str(tmp_path / "run_manifest"),
        },
    }
    collect_auxiliary_outputs = build_pipeline.__dict__["_collect_auxiliary_outputs"]

    outputs = collect_auxiliary_outputs(
        output_dir=tmp_path,
        artifacts=artifacts,
        extraction_result=extraction_result,
        options=_AuxiliaryOutputOptions(
            include_errors=True,
            include_manifest=True,
            include_run_bundle=True,
        ),
    )

    assert outputs["normalize_outputs_delta"]["path"] == str(tmp_path / "normalize_outputs")
    assert outputs["write_normalize_outputs_delta"]["path"] == str(tmp_path / "normalize_outputs")
    assert outputs["extract_error_artifacts_delta"]["rows"] == 1
    assert outputs["run_manifest_delta"]["path"] == str(tmp_path / "run_manifest")
    assert outputs["run_bundle_dir"]["bundle_dir"] == str(tmp_path / "run_bundle")
    assert outputs["write_run_bundle_dir"]["bundle_dir"] == str(tmp_path / "run_bundle")


def test_collect_auxiliary_outputs_falls_back_to_output_dir(tmp_path: Path) -> None:
    """Test collect auxiliary outputs falls back to output dir."""
    extraction_result = ExtractionResult(
        delta_locations={},
        semantic_input_locations={},
        errors=[],
        timing={},
    )
    collect_auxiliary_outputs = build_pipeline.__dict__["_collect_auxiliary_outputs"]
    outputs = collect_auxiliary_outputs(
        output_dir=tmp_path,
        artifacts={},
        extraction_result=extraction_result,
        options=_AuxiliaryOutputOptions(
            include_errors=False,
            include_manifest=True,
            include_run_bundle=True,
        ),
    )

    assert outputs["normalize_outputs_delta"]["path"] == str(tmp_path / "normalize_outputs")
    assert outputs["run_manifest_delta"]["path"] == str(tmp_path / "run_manifest")
    assert outputs["run_bundle_dir"]["bundle_dir"] == str(tmp_path / "run_bundle")


def test_execute_engine_phase_preserves_deterministic_plan_artifact_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Engine boundary keeps deterministic optimizer fields in run_result."""

    class _EngineModule:
        @staticmethod
        def run_build(_payload: str) -> dict[str, object]:
            return {
                "run_result": {
                    "plan_bundles": [
                        {
                            "artifacts": {
                                "deterministic_optimizer": True,
                                "optimizer_traces": [
                                    {"step_index": 0, "rule_name": "FilterPushDown"}
                                ],
                            }
                        }
                    ]
                },
                "artifacts": {
                    "manifest_path": str(tmp_path / "run_manifest"),
                    "auxiliary_outputs": {
                        "run_manifest_delta": str(tmp_path / "run_manifest"),
                    },
                },
            }

    monkeypatch.setattr(build_pipeline.importlib, "import_module", lambda _name: _EngineModule())

    spec = SemanticExecutionSpec(
        version=1,
        input_relations=(InputRelation(logical_name="input", delta_location="/tmp/in"),),
        view_definitions=(
            ViewDefinition(
                name="view",
                view_kind="project",
                transform={"source": "input"},
            ),
        ),
        join_graph=JoinGraph(),
        output_targets=(
            OutputTarget(
                table_name="out",
                source_view="view",
                delta_location=str(tmp_path / "out"),
            ),
        ),
        rule_intents=(),
        rulepack_profile="Default",
    )

    run_result, artifacts = build_pipeline._execute_engine_phase(
        {"input": "/tmp/in"},
        spec,
        "medium",
    )

    plan_bundles = run_result["plan_bundles"]
    assert isinstance(plan_bundles, list)
    artifact_payload = plan_bundles[0]["artifacts"]
    assert artifact_payload["deterministic_optimizer"] is True
    assert artifact_payload["optimizer_traces"][0]["rule_name"] == "FilterPushDown"
    assert artifacts["manifest_path"] == str(tmp_path / "run_manifest")
