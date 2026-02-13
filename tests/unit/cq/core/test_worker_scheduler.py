"""Unit tests for CQ worker scheduler policies."""

from __future__ import annotations

import threading
import time

from tools.cq.core.runtime.execution_policy import ParallelismPolicy
from tools.cq.core.runtime.worker_scheduler import WorkerScheduler


def test_submit_lsp_respects_lsp_request_workers_limit() -> None:
    scheduler = WorkerScheduler(
        ParallelismPolicy(
            cpu_workers=1,
            io_workers=4,
            lsp_request_workers=1,
            enable_process_pool=False,
        )
    )
    lock = threading.Lock()
    active = 0
    max_active = 0

    def _task() -> bool:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return True

    futures = [scheduler.submit_lsp(_task) for _ in range(5)]
    results = [future.result(timeout=1.0) for future in futures]
    scheduler.close()

    assert all(results)
    assert max_active == 1
