"""Shared option mixins for extraction modules."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RepoOptions:
    """Repository identifier options."""

    repo_id: str | None = None


@dataclass(frozen=True)
class WorklistQueueOptions:
    """Worklist queue selection options."""

    use_worklist_queue: bool = True


@dataclass(frozen=True)
class WorkerOptions:
    """Worker pool options."""

    max_workers: int | None = None


@dataclass(frozen=True)
class BatchOptions:
    """Batch sizing options."""

    batch_size: int | None = None


@dataclass(frozen=True)
class ParallelOptions(WorkerOptions, BatchOptions):
    """Parallel execution options."""

    batch_size: int | None = 512
    parallel: bool = True


__all__ = [
    "BatchOptions",
    "ParallelOptions",
    "RepoOptions",
    "WorkerOptions",
    "WorklistQueueOptions",
]
