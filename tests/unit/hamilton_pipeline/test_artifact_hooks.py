"""Tests for artifact hooks that write snapshot outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

from core_types import JsonValue
from hamilton_pipeline.graph_snapshot import GraphSnapshotHook
from hamilton_pipeline.semantic_registry import SemanticRegistryHook

if TYPE_CHECKING:
    from hamilton.driver import Driver

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


class _DummyDriver:
    def visualize_execution_graph(self, output_file_path: str) -> None:
        Path(output_file_path).write_text("ok", encoding="utf-8")


class _DummyNode:
    def __init__(self, name: str, tags: Mapping[str, object]) -> None:
        self.name = name
        self.tags = dict(tags)


class _DummyRegistryDriver:
    def __init__(self, nodes: Sequence[_DummyNode]) -> None:
        self._nodes = tuple(nodes)

    def list_available_variables(
        self,
        tag_filter: Mapping[str, object] | None = None,
    ) -> Sequence[_DummyNode]:
        if tag_filter and tag_filter.get("layer") == "semantic":
            return self._nodes
        return self._nodes


def test_graph_snapshot_hook_writes_snapshot(tmp_path: Path) -> None:
    """Graph snapshots should be emitted to the cache path."""
    plan_signature = "plan_signature_test"
    config: Mapping[str, JsonValue] = {"cache_path": str(tmp_path)}
    hook = GraphSnapshotHook(
        profile=cast("DataFusionRuntimeProfile", None),
        plan_signature=plan_signature,
        config=config,
    )
    hook.bind_driver(cast("Driver", _DummyDriver()))
    hook.run_before_graph_execution(run_id="run_id")
    expected_path = tmp_path / "graph_snapshots" / f"{plan_signature}.png"
    assert expected_path.exists()


def test_semantic_registry_hook_writes_registry(tmp_path: Path) -> None:
    """Semantic registry hook should emit a registry artifact file."""
    plan_signature = "plan_signature_test"
    run_id = "run_id"
    config: Mapping[str, JsonValue] = {"cache_path": str(tmp_path)}
    hook = SemanticRegistryHook(
        profile=cast("DataFusionRuntimeProfile", None),
        plan_signature=plan_signature,
        config=config,
    )
    tags = {
        "layer": "semantic",
        "artifact": "semantic_table",
        "semantic_id": "semantic.table.v1",
        "kind": "table",
        "entity": "node",
        "grain": "per_node",
        "version": "1",
        "stability": "stable",
        "schema_ref": "cpg_nodes_v1",
        "entity_keys": ("node_id",),
        "join_keys": ("node_id",),
    }
    driver = _DummyRegistryDriver([_DummyNode("semantic_table", tags)])
    hook.bind_driver(cast("Driver", driver))
    hook.run_before_graph_execution(run_id=run_id)
    expected_path = tmp_path / "lineage" / run_id / f"semantic_registry_{plan_signature}.json"
    assert expected_path.exists()
