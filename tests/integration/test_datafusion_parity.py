"""Parity checks for DataFusion bridge execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import pytest

from arrowdsl.core.context import execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from config import AdapterMode
from cpg.registry_specs import dataset_contract_spec
from hamilton_pipeline import build_driver
from hamilton_pipeline.pipeline_types import ScipIndexConfig
from tests.utils import table_digest

TableOutput = TableLike | RecordBatchReaderLike

_CPG_OUTPUTS: tuple[str, ...] = ("cpg_nodes", "cpg_edges", "cpg_props")
_CPG_DATASETS: dict[str, str] = {
    "cpg_nodes": "cpg_nodes_v1",
    "cpg_edges": "cpg_edges_v1",
    "cpg_props": "cpg_props_v1",
}


@pytest.mark.integration
def test_datafusion_bridge_parity(tmp_path: Path) -> None:
    """Ensure bridge on/off outputs are schema- and digest-equal."""
    repo_root = _fixture_repo_root()
    outputs_off = _run_pipeline(
        repo_root,
        use_datafusion_bridge=False,
        scip_output_dir=tmp_path / "scip_off",
    )
    outputs_on = _run_pipeline(
        repo_root,
        use_datafusion_bridge=True,
        scip_output_dir=tmp_path / "scip_on",
    )
    for name in _CPG_OUTPUTS:
        table_off = outputs_off[name]
        table_on = outputs_on[name]
        assert table_off.schema == table_on.schema
        sort_keys = _canonical_sort_keys(name)
        assert table_digest(table_off, sort_keys=sort_keys) == table_digest(
            table_on,
            sort_keys=sort_keys,
        )


def _run_pipeline(
    repo_root: Path,
    *,
    use_datafusion_bridge: bool,
    scip_output_dir: Path,
) -> Mapping[str, TableOutput]:
    driver = build_driver(config={})
    ctx = execution_context_factory("default")
    overrides = {
        "adapter_mode": AdapterMode(
            use_ibis_bridge=True,
            use_datafusion_bridge=use_datafusion_bridge,
        ),
        "ctx": ctx,
        "scip_index_config": ScipIndexConfig(
            enabled=False,
            output_dir=str(scip_output_dir),
        ),
    }
    outputs = driver.execute(
        list(_CPG_OUTPUTS),
        overrides=overrides,
        inputs={"repo_root": str(repo_root)},
    )
    return cast("Mapping[str, TableOutput]", outputs)


def _canonical_sort_keys(output_name: str) -> Sequence[tuple[str, str]]:
    dataset_name = _CPG_DATASETS[output_name]
    contract = dataset_contract_spec(dataset_name)
    return tuple((key.column, key.order) for key in contract.canonical_sort)


def _fixture_repo_root() -> Path:
    root = Path(__file__).resolve().parents[1] / "fixtures" / "repos" / "mini_repo"
    if root.exists():
        return root
    msg = f"Fixture repo not found at {root}"
    raise FileNotFoundError(msg)
