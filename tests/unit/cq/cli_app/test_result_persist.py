"""Tests for CLI result artifact persistence helpers."""

from __future__ import annotations

from tools.cq.cli_app.result_persist import persist_result_artifacts
from tools.cq.core.schema import CqResult, RunMeta


def _search_result(run_id: str | None = "run-1") -> CqResult:
    return CqResult(
        run=RunMeta(
            macro="search",
            argv=["cq", "search", "target"],
            root=".",
            started_ms=0.0,
            elapsed_ms=1.0,
            toolchain={},
            run_id=run_id,
        )
    )


def test_persist_result_artifacts_no_save_pops_search_view() -> None:
    result = _search_result("rid")
    popped: list[str] = []

    def _pop(run_id: str) -> object | None:
        popped.append(run_id)
        return None

    persist_result_artifacts(
        result=result,
        artifact_dir=None,
        no_save=True,
        pop_search_object_view_for_run=_pop,
    )

    assert popped == ["rid"]
    assert result.artifacts == []


def test_persist_result_artifacts_no_save_ignores_non_search() -> None:
    result = _search_result("rid")
    result.run.macro = "q"
    popped: list[str] = []

    persist_result_artifacts(
        result=result,
        artifact_dir=None,
        no_save=True,
        pop_search_object_view_for_run=lambda run_id: popped.append(run_id),
    )

    assert popped == []
