"""End-to-end pipeline run against the current repo."""

from __future__ import annotations

import shutil
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


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _assert_cpg_outputs(results: Mapping[str, object], output_dir: Path) -> None:
    for key in ("write_cpg_nodes_parquet", "write_cpg_edges_parquet", "write_cpg_props_parquet"):
        report = cast("dict[str, object] | None", results.get(key))
        assert report is not None
        paths = cast("dict[str, str]", report.get("paths"))
        for path in paths.values():
            assert Path(path).exists()
    assert (output_dir / "cpg_nodes.parquet").exists()
    assert (output_dir / "cpg_edges.parquet").exists()
    assert (output_dir / "cpg_props.parquet").exists()


def _assert_extract_errors(results: Mapping[str, object]) -> None:
    report = cast("dict[str, object] | None", results.get("write_extract_error_artifacts_parquet"))
    assert report is not None
    base_dir = Path(cast("str", report.get("base_dir")))
    assert base_dir.exists()
    datasets = cast("dict[str, dict[str, object]]", report.get("datasets"))
    for payload in datasets.values():
        paths = cast("dict[str, str]", payload.get("paths"))
        for path in paths.values():
            assert Path(path).exists()


def _assert_manifest_and_bundle(results: Mapping[str, object]) -> None:
    manifest = cast("dict[str, object] | None", results.get("write_run_manifest_json"))
    assert manifest is not None
    manifest_path = Path(cast("str", manifest.get("path")))
    assert manifest_path.exists()

    bundle = cast("dict[str, object] | None", results.get("write_run_bundle_dir"))
    assert bundle is not None
    bundle_dir = Path(cast("str", bundle.get("bundle_dir")))
    assert bundle_dir.exists()
    files_written = cast("list[str]", bundle.get("files_written"))
    assert files_written


def _assert_scip_index(repo_root: Path) -> None:
    index_path = repo_root / "build" / "scip" / "index.scip"
    assert index_path.exists()
    assert index_path.stat().st_size > 0
