"""Diskcache-native coordination primitives for CQ runtime lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext

from tools.cq.core.cache.interface import CqCacheBackend, CqCacheCoordinationBackend


@contextmanager
def tree_sitter_lane_guard(
    *,
    backend: CqCacheBackend,
    lock_key: str,
    semaphore_key: str,
    lane_limit: int,
    ttl_seconds: int,
) -> Iterator[None]:
    """Guard one tree-sitter lane with diskcache semaphore + lock primitives."""
    if lane_limit <= 0:
        with nullcontext():
            yield
        return

    if not isinstance(backend, CqCacheCoordinationBackend):
        with nullcontext():
            yield
        return

    expire = max(1, int(ttl_seconds))
    with (
        backend.semaphore(semaphore_key, value=max(1, int(lane_limit)), expire=expire),
        backend.lock(lock_key, expire=expire),
        backend.rlock(f"{lock_key}:reentrant", expire=expire),
    ):
        yield


def publish_once_per_barrier(
    *,
    backend: CqCacheBackend,
    barrier_key: str,
    publish_fn: Callable[[], None],
) -> None:
    """Run publish function under diskcache `barrier` coordination when available."""
    if not isinstance(backend, CqCacheCoordinationBackend):
        publish_fn()
        return
    backend.barrier(barrier_key, publish_fn)


__all__ = [
    "publish_once_per_barrier",
    "tree_sitter_lane_guard",
]
