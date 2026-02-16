"""Unit tests for run_build artifact-contract consumption in build_pipeline."""

from __future__ import annotations

from pathlib import Path

from extraction.orchestrator import ExtractionResult
from graph import build_pipeline
from graph.build_pipeline import _AuxiliaryOutputOptions


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

    outputs = build_pipeline._collect_auxiliary_outputs(  # noqa: SLF001
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
    outputs = build_pipeline._collect_auxiliary_outputs(  # noqa: SLF001
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
