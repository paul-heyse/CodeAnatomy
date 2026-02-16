"""Parallel job execution helpers for tree-sitter lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed


def run_file_lanes_parallel[T, R](
    jobs: Iterable[T],
    *,
    worker: Callable[[T], R],
    max_workers: int,
) -> list[R]:
    """Execute independent file jobs in parallel and preserve input order.

    Returns:
        list[R]: Job results in the same order as the input jobs.
    """
    job_list = list(jobs)
    if not job_list:
        return []
    if max_workers <= 1:
        return [worker(job) for job in job_list]

    indexed_results: dict[int, R] = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {executor.submit(worker, job): idx for idx, job in enumerate(job_list)}
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            indexed_results[idx] = future.result()
    return [indexed_results[idx] for idx in range(len(job_list))]


__all__ = ["run_file_lanes_parallel"]
