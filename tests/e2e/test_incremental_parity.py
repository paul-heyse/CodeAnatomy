"""Incremental pipeline parity checks against full runs."""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from arrowdsl.schema.serialization import schema_fingerprint
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

    for name in ("cpg_nodes.parquet", "cpg_edges.parquet", "cpg_props.parquet"):
        _assert_parquet_parity(full_dir / name, incremental_dir / name)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _assert_parquet_parity(full_path: Path, incremental_path: Path) -> None:
    assert full_path.exists()
    assert incremental_path.exists()
    full_summary = _parquet_summary(full_path)
    incremental_summary = _parquet_summary(incremental_path)
    assert full_summary == incremental_summary


def _parquet_summary(path: Path) -> tuple[int | None, str]:
    pf = pq.ParquetFile(path)
    rows = pf.metadata.num_rows if pf.metadata is not None else None
    return rows, schema_fingerprint(pf.schema_arrow)
