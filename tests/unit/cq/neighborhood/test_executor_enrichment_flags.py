"""Tests for neighborhood executor enrichment flag propagation."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from tools.cq.core.schema import CqResult, RunMeta
from tools.cq.core.summary_contract import summary_from_mapping
from tools.cq.core.toolchain import Toolchain
from tools.cq.neighborhood.bundle_builder import BundleBuildRequest
from tools.cq.neighborhood.executor import NeighborhoodExecutionRequest, execute_neighborhood
from tools.cq.search.pipeline.enrichment_contracts import IncrementalEnrichmentModeV1


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
        py_version="3.13.12",
    )


def _render_result(run: RunMeta) -> CqResult:
    return CqResult(run=run, summary=summary_from_mapping({}))


def test_neighborhood_executor_propagates_incremental_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Propagate incremental enrichment flags from request to rendered summary."""
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
    captured: dict[str, BundleBuildRequest] = {}

    def _fake_bundle(request: BundleBuildRequest) -> dict[str, object]:
        captured["request"] = request
        return {"bundle": True}

    monkeypatch.setattr("tools.cq.neighborhood.executor.build_neighborhood_bundle", _fake_bundle)
    monkeypatch.setattr(
        "tools.cq.neighborhood.executor.semantic_env_from_bundle",
        lambda _bundle: {"semantic": True},
    )
    monkeypatch.setattr(
        "tools.cq.neighborhood.executor.render_snb_result",
        lambda request: _render_result(request.run),
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
            incremental_enrichment_enabled=False,
            incremental_enrichment_mode=IncrementalEnrichmentModeV1.FULL,
            run_id="run-123",
        )
    )

    request = captured["request"]
    assert request.incremental_enrichment_enabled is False
    assert request.incremental_enrichment_mode == "full"
    assert result.summary.get("incremental_enrichment_mode") == "full"
