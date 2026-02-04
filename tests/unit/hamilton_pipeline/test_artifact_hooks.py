"""Tests for artifact hooks that write snapshot outputs."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, cast

from core_types import JsonValue
from hamilton_pipeline.graph_snapshot import GraphSnapshotHook

if TYPE_CHECKING:
    from hamilton.driver import Driver

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class _DummyDriver:
    def visualize_execution(
        self,
        final_vars: list[str],
        *,
        output_file_path: str | None = None,
        inputs: Mapping[str, object] | None = None,
        overrides: Mapping[str, object] | None = None,
    ) -> None:
        _ = final_vars, inputs, overrides
        if output_file_path is None:
            msg = "output_file_path is required for graph snapshots."
            raise ValueError(msg)
        Path(output_file_path).write_text("ok", encoding="utf-8")


def test_graph_snapshot_hook_writes_snapshot(tmp_path: Path) -> None:
    """Graph snapshots should be emitted to the cache path."""
    plan_signature = "plan_signature_test"
    config: Mapping[str, JsonValue] = {"cache": {"path": str(tmp_path)}}
    hook = GraphSnapshotHook(
        profile=cast("DataFusionRuntimeProfile", None),
        plan_signature=plan_signature,
        config=config,
    )
    hook.bind_driver(cast("Driver", _DummyDriver()))
    hook.run_before_graph_execution(
        run_id="run_id",
        final_vars=["write_cpg_nodes_delta"],
        inputs={},
        overrides={},
        graph=object(),
        execution_path=(),
    )
    expected_path = tmp_path / "graph_snapshots" / f"{plan_signature}.png"
    assert expected_path.exists()
