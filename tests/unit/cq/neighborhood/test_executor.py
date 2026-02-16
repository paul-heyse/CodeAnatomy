"""Tests for shared neighborhood executor."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.summary_contract import summary_from_mapping
from tools.cq.core.toolchain import Toolchain
from tools.cq.neighborhood.executor import NeighborhoodExecutionRequest, execute_neighborhood
from tools.cq.neighborhood.snb_renderer import RenderSnbRequest


def _toolchain() -> Toolchain:
    return Toolchain(
        rg_available=True,
        rg_version="rg 14",
        sgpy_available=True,
        sgpy_version="1.0",
        msgspec_available=True,
        msgspec_version="0.18",
        diskcache_available=True,
        diskcache_version="5.6",
        py_path="python",
        py_version="3.13.0",
        rg_pcre2_available=True,
        rg_pcre2_version="10.0",
    )


def _render_result(run: RunMeta) -> CqResult:
    return CqResult(run=run, summary=summary_from_mapping({}))


def test_execute_neighborhood_sets_resolution_and_evicts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Neighborhood executor forwards target/language and records cache eviction."""
    resolved = SimpleNamespace(
        target_name="build_graph",
        target_file="src/app.py",
        target_line=10,
        target_col=2,
        target_uri=None,
        symbol_hint=None,
        degrade_events=(),
        resolution_kind="symbol",
    )
    monkeypatch.setattr("tools.cq.neighborhood.executor.parse_target_spec", lambda target: target)
    monkeypatch.setattr(
        "tools.cq.neighborhood.executor.resolve_target", lambda *_args, **_kwargs: resolved
    )
    monkeypatch.setattr(
        "tools.cq.neighborhood.executor.build_neighborhood_bundle",
        lambda _request: {"bundle": True},
    )
    monkeypatch.setattr(
        "tools.cq.neighborhood.executor.semantic_env_from_bundle",
        lambda _bundle: {"semantic": True},
    )

    captured: dict[str, object] = {}

    def _fake_render(request: RenderSnbRequest) -> CqResult:
        captured["target"] = request.target
        captured["language"] = request.language
        return _render_result(request.run)

    monkeypatch.setattr("tools.cq.neighborhood.executor.render_snb_result", _fake_render)
    evicted: dict[str, object] = {}
    monkeypatch.setattr(
        "tools.cq.neighborhood.executor.maybe_evict_run_cache_tag",
        lambda **kwargs: evicted.update(kwargs),
    )

    result = execute_neighborhood(
        NeighborhoodExecutionRequest(
            target="build_graph",
            root=tmp_path,
            argv=["cq", "neighborhood", "build_graph"],
            toolchain=_toolchain(),
            lang="python",
            top_k=5,
            semantic_enrichment=True,
            run_id="run-123",
        )
    )

    assert result.summary.target_resolution_kind == "symbol"
    assert captured["target"] == "build_graph"
    assert captured["language"] == "python"
    assert evicted["language"] == "python"
    assert evicted["run_id"] == "run-123"
