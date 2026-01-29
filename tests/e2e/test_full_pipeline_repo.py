"""End-to-end pipeline run against the current repo."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest

from hamilton_pipeline import PipelineExecutionOptions, execute_pipeline
from hamilton_pipeline.pipeline_types import ScipIndexConfig


@pytest.mark.e2e
@pytest.mark.serial
def test_full_pipeline_repo() -> None:
    """Run the full pipeline against the current repo and assert artifacts exist."""
    repo_root = _repo_root()
    output_dir = repo_root / "build" / "e2e_full_pipeline"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    options = PipelineExecutionOptions(
        output_dir=output_dir,
        scip_index_config=ScipIndexConfig(output_dir="build/scip"),
    )
    results = execute_pipeline(repo_root=repo_root, options=options)

    _assert_cpg_outputs(results, output_dir)
    _assert_extract_errors(results)
    _assert_manifest_and_bundle(results)
    _assert_scip_index(repo_root)
    _run_diagnostics_report(repo_root, output_dir)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _assert_cpg_outputs(results: Mapping[str, object], output_dir: Path) -> None:
    for key in ("write_cpg_nodes_delta", "write_cpg_edges_delta", "write_cpg_props_delta"):
        report = cast("dict[str, object] | None", results.get(key))
        assert report is not None
        paths = cast("dict[str, str]", report.get("paths"))
        for path in paths.values():
            assert Path(path).exists()
    assert (output_dir / "cpg_nodes").exists()
    assert (output_dir / "cpg_edges").exists()
    assert (output_dir / "cpg_props").exists()
    for key, dirname in (
        ("write_cpg_props_map_delta", "cpg_props_map"),
        ("write_cpg_edges_by_src_delta", "cpg_edges_by_src"),
        ("write_cpg_edges_by_dst_delta", "cpg_edges_by_dst"),
    ):
        report = cast("dict[str, object] | None", results.get(key))
        assert report is not None
        path = Path(cast("str", report.get("path")))
        assert path.exists()
        assert (output_dir / dirname).exists()


def _assert_extract_errors(results: Mapping[str, object]) -> None:
    report = cast("dict[str, object] | None", results.get("write_extract_error_artifacts_delta"))
    assert report is not None
    path = Path(cast("str", report.get("path")))
    assert path.exists()
    assert report.get("rows") == 1


def _assert_manifest_and_bundle(results: Mapping[str, object]) -> None:
    manifest = cast("dict[str, object] | None", results.get("write_run_manifest_delta"))
    assert manifest is not None
    manifest_path = Path(cast("str", manifest.get("path")))
    assert manifest_path.exists()

    bundle = cast("dict[str, object] | None", results.get("write_run_bundle_dir"))
    assert bundle is not None
    bundle_dir = Path(cast("str", bundle.get("bundle_dir")))
    assert bundle_dir.exists()
    run_id = cast("str | None", bundle.get("run_id"))
    assert run_id


def _assert_scip_index(repo_root: Path) -> None:
    index_path = repo_root / "build" / "scip" / "index.scip"
    assert index_path.exists()
    assert index_path.stat().st_size > 0


def _run_diagnostics_report(repo_root: Path, output_dir: Path) -> None:
    report_path = output_dir / "diagnostics_report.json"
    command = [
        sys.executable,
        str(repo_root / "scripts" / "e2e_diagnostics_report.py"),
        "--output-dir",
        str(output_dir),
        "--validate-delta",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload.get("status") == "ok"
