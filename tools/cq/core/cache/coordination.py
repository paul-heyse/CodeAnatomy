"""Diskcache-native coordination primitives for CQ runtime lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext

from tools.cq.core.cache.interface import CqCacheBackend


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

    lock_fn = getattr(backend, "lock", None)
    rlock_fn = getattr(backend, "rlock", None)
    semaphore_fn = getattr(backend, "semaphore", None)
    if not callable(lock_fn) or not callable(rlock_fn) or not callable(semaphore_fn):
        with nullcontext():
            yield
        return

    expire = max(1, int(ttl_seconds))
    with (
        semaphore_fn(semaphore_key, value=max(1, int(lane_limit)), expire=expire),
        lock_fn(lock_key, expire=expire),
        rlock_fn(f"{lock_key}:reentrant", expire=expire),
    ):
        yield


def publish_once_per_barrier(
    *,
    backend: CqCacheBackend,
    barrier_key: str,
    publish_fn: Callable[[], None],
) -> None:
    """Run publish function under diskcache `barrier` coordination when available."""
    barrier_fn = getattr(backend, "barrier", None)
    if not callable(barrier_fn):
        publish_fn()
        return
    barrier_fn(barrier_key, publish_fn)


__all__ = [
    "publish_once_per_barrier",
    "tree_sitter_lane_guard",
]
