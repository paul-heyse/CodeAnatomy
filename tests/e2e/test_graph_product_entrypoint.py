"""End-to-end graph product entrypoint coverage."""

from __future__ import annotations

from pathlib import Path

import pytest

from graph import GraphProductBuildRequest, build_graph_product
from hamilton_pipeline.pipeline_types import ScipIndexConfig


@pytest.mark.e2e
@pytest.mark.serial
def test_graph_product_build_entrypoint(tmp_path: Path) -> None:
    """Build the graph product via the public entrypoint."""
    repo_root = Path(__file__).resolve().parents[2]
    output_dir = tmp_path / "graph_product"
    result = build_graph_product(
        GraphProductBuildRequest(
            repo_root=repo_root,
            output_dir=output_dir,
            scip_index_config=ScipIndexConfig(enabled=False),
        )
    )

    assert result.cpg_nodes.paths.data.exists()
    assert result.cpg_edges.paths.data.exists()
    assert result.cpg_props.paths.data.exists()
    assert (output_dir / "cpg_nodes.parquet").exists()
    assert (output_dir / "cpg_edges.parquet").exists()
    assert (output_dir / "cpg_props.parquet").exists()
