"""Tests for cache-policy derivation through the Rust bridge surface."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from datafusion_engine.extensions import datafusion_ext
from relspec import policy_compiler


@dataclass(frozen=True)
class _Graph:
    out_degree_map: dict[int, int]

    def out_degree(self, node_idx: int) -> int:
        return self.out_degree_map[node_idx]


@dataclass(frozen=True)
class _TaskGraph:
    task_idx: dict[str, int]
    graph: _Graph

    def out_degree(self, task_name: str) -> int:
        return self.graph.out_degree(self.task_idx[task_name])


def test_derive_cache_policies_prefers_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bridge payload is used when derive_cache_policies is available."""
    task_graph = _TaskGraph(
        task_idx={"a": 0, "b": 1},
        graph=_Graph(out_degree_map={0: 0, 1: 3}),
    )

    captured: dict[str, object] = {}

    def _fake_bridge(payload: dict[str, object]) -> dict[str, str]:
        captured.update(payload)
        return {"a": "delta_output", "b": "none"}

    monkeypatch.setattr(datafusion_ext, "derive_cache_policies", _fake_bridge, raising=False)

    policies = policy_compiler.derive_cache_policies_from_graph(
        task_graph,
        output_locations={"a": object()},
        cache_overrides=None,
        workload_class="compile_replay",
    )

    assert policies == {"a": "delta_output", "b": "none"}
    assert captured["outputs"] == ["a"]


def test_derive_cache_policies_raises_when_bridge_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bridge failures are surfaced in hard-cutover mode."""
    task_graph = _TaskGraph(
        task_idx={"leaf": 0, "fanout": 1},
        graph=_Graph(out_degree_map={0: 0, 1: 3}),
    )

    def _broken_bridge(_payload: dict[str, object]) -> dict[str, str]:
        msg = "bridge unavailable"
        raise RuntimeError(msg)

    monkeypatch.setattr(datafusion_ext, "derive_cache_policies", _broken_bridge, raising=False)

    with pytest.raises(RuntimeError, match="derive_cache_policies bridge"):
        _ = policy_compiler.derive_cache_policies_from_graph(
            task_graph,
            output_locations={},
            cache_overrides=None,
            workload_class=None,
        )
