from __future__ import annotations

import time

from pytest import MonkeyPatch
from tools.cq.core.multilang_orchestrator import execute_by_language_scope
from tools.cq.core.runtime.worker_scheduler import close_worker_scheduler


def test_execute_by_language_scope_parallel_preserves_language_order(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("CQ_RUNTIME_QUERY_PARTITION_WORKERS", "2")
    close_worker_scheduler()

    def run_one(language: str) -> str:
        if language == "python":
            time.sleep(0.05)
        else:
            time.sleep(0.01)
        return f"{language}-done"

    results = execute_by_language_scope("auto", run_one)

    assert list(results.keys()) == ["python", "rust"]
    assert results["python"] == "python-done"
    assert results["rust"] == "rust-done"
