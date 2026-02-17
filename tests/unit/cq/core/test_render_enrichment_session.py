"""Tests for render-enrichment session orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tools.cq.core.render_enrichment_orchestrator import (
    RenderEnrichmentTask,
    prepare_render_enrichment_session,
)
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import mk_result
from tools.cq.core.toolchain import Toolchain


def _result(root: Path) -> Any:
    run_ctx = RunContext.from_parts(
        root=root,
        argv=["cq", "search"],
        tc=Toolchain.detect(),
        started_ms=0.0,
        run_id="run-1",
    )
    return mk_result(run_ctx.to_runmeta("search"))


def test_prepare_render_enrichment_session_builds_metrics_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Session preparation should compute attempted/applied/failed/skipped metrics."""
    result = _result(tmp_path)
    task_ok = RenderEnrichmentTask(
        root=str(tmp_path),
        file="a.py",
        line=1,
        col=0,
        language="python",
        candidates=("target",),
    )
    task_missing = RenderEnrichmentTask(
        root=str(tmp_path),
        file="b.py",
        line=2,
        col=1,
        language="python",
        candidates=("other",),
    )

    monkeypatch.setattr(
        "tools.cq.core.render_enrichment_orchestrator.select_enrichment_target_files",
        lambda _result: {"a.py", "b.py"},
    )
    monkeypatch.setattr(
        "tools.cq.core.render_enrichment_orchestrator.count_render_enrichment_tasks",
        lambda **_kwargs: 5,
    )

    def _fake_precompute_render_enrichment_cache(**kwargs: Any) -> list[RenderEnrichmentTask]:
        cache = kwargs["cache"]
        cache[task_ok.file, task_ok.line, task_ok.col, task_ok.language] = {"ok": True}
        return [task_ok, task_missing]

    monkeypatch.setattr(
        "tools.cq.core.render_enrichment_orchestrator.precompute_render_enrichment_cache",
        _fake_precompute_render_enrichment_cache,
    )
    monkeypatch.setattr(
        "tools.cq.core.render_enrichment_orchestrator._summary_with_render_enrichment_metrics",
        lambda _summary, **metrics: metrics,
    )

    session = prepare_render_enrichment_session(result=result, root=tmp_path, port=None)

    assert session.allowed_files == {"a.py", "b.py"}
    assert session.summary_with_metrics == {
        "attempted": 2,
        "applied": 1,
        "failed": 1,
        "skipped": 3,
    }
