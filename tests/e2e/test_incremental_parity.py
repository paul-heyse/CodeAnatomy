"""Incremental pipeline parity checks against full runs."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from datafusion_engine.arrow_schema.abi import schema_fingerprint
from datafusion_engine.runtime import read_delta_as_reader
from hamilton_pipeline import PipelineExecutionOptions, execute_pipeline
from hamilton_pipeline.pipeline_types import ScipIndexConfig
from incremental.types import IncrementalConfig


@pytest.mark.e2e
@pytest.mark.serial
def test_incremental_parity_full_refresh() -> None:
    """Ensure incremental full-refresh output matches the full pipeline."""
    repo_root = _repo_root()
    base_dir = repo_root / "build" / "e2e_incremental_parity"
    if base_dir.exists():
        shutil.rmtree(base_dir)
    full_dir = base_dir / "full"
    incremental_dir = base_dir / "incremental"
    state_dir = base_dir / "state"
    scip_dir = base_dir / "scip"

    execute_pipeline(
        repo_root=repo_root,
        options=PipelineExecutionOptions(
            output_dir=full_dir,
            scip_index_config=ScipIndexConfig(output_dir=str(scip_dir)),
        ),
    )

    execute_pipeline(
        repo_root=repo_root,
        options=PipelineExecutionOptions(
            output_dir=incremental_dir,
            scip_index_config=ScipIndexConfig(output_dir=str(scip_dir)),
            incremental_config=IncrementalConfig(
                enabled=True,
                state_dir=state_dir,
                repo_id="parity_repo",
            ),
        ),
    )

    for name in ("cpg_nodes", "cpg_edges", "cpg_props"):
        _assert_delta_parity(full_dir / name, incremental_dir / name)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _assert_delta_parity(full_path: Path, incremental_path: Path) -> None:
    assert full_path.exists()
    assert incremental_path.exists()
    full_summary = _delta_summary(full_path)
    incremental_summary = _delta_summary(incremental_path)
    assert full_summary == incremental_summary


def _delta_summary(path: Path) -> tuple[int, str]:
    table = read_delta_as_reader(str(path)).read_all()
    return int(table.num_rows), schema_fingerprint(table.schema)
