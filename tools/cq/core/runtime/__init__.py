"""Runtime primitives for CQ execution policies and worker orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tools.cq.core.cache.policy import CqCachePolicyV1
    from tools.cq.core.runtime.execution_policy import (
        ParallelismPolicy,
        RuntimeExecutionPolicy,
        SemanticRuntimePolicy,
        default_runtime_execution_policy,
    )
    from tools.cq.core.runtime.worker_scheduler import (
        WorkerBatchResult,
        WorkerScheduler,
        close_worker_scheduler,
        get_worker_scheduler,
        set_worker_scheduler,
    )

__all__ = [
    "CqCachePolicyV1",
    "ParallelismPolicy",
    "RuntimeExecutionPolicy",
    "SemanticRuntimePolicy",
    "WorkerBatchResult",
    "WorkerScheduler",
    "close_worker_scheduler",
    "default_runtime_execution_policy",
    "get_worker_scheduler",
    "set_worker_scheduler",
]


def __getattr__(name: str) -> Any:
    """Resolve runtime exports lazily to avoid package-import cycles.

    Returns:
        Any: Exported runtime symbol value.

    Raises:
        AttributeError: Raised when ``name`` is not a known export.
    """
    execution_policy_exports = {
        "CqCachePolicyV1",
        "ParallelismPolicy",
        "RuntimeExecutionPolicy",
        "SemanticRuntimePolicy",
        "default_runtime_execution_policy",
    }
    if name in execution_policy_exports:
        from tools.cq.core.runtime import execution_policy as module

        return getattr(module, name)

    worker_scheduler_exports = {
        "WorkerBatchResult",
        "WorkerScheduler",
        "close_worker_scheduler",
        "get_worker_scheduler",
        "set_worker_scheduler",
    }
    if name in worker_scheduler_exports:
        from tools.cq.core.runtime import worker_scheduler as module

        return getattr(module, name)

    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
