"""Diskcache-native coordination primitives for CQ runtime lanes."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext, suppress
from typing import TYPE_CHECKING

from tools.cq.core.cache.interface import CqCacheBackend

if TYPE_CHECKING:
    from diskcache import FanoutCache

try:
    from diskcache import BoundedSemaphore, Lock, RLock, barrier
except ImportError:  # pragma: no cover - optional dependency
    BoundedSemaphore = None
    Lock = None
    RLock = None
    barrier = None


def _fanout_cache(backend: CqCacheBackend) -> FanoutCache | None:
    cache = getattr(backend, "cache", None)
    if cache is None:
        return None
    try:
        # Attribute-based check avoids importing FanoutCache at type-check runtime.
        _ = cache.get
        _ = cache.set
    except AttributeError:
        return None
    return cache


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
    cache = _fanout_cache(backend)
    if (
        cache is None
        or BoundedSemaphore is None
        or Lock is None
        or RLock is None
        or lane_limit <= 0
    ):
        with nullcontext():
            yield
        return

    semaphore = BoundedSemaphore(
        cache,
        semaphore_key,
        value=max(1, int(lane_limit)),
        expire=max(1, int(ttl_seconds)),
    )
    lock = Lock(cache, lock_key, expire=max(1, int(ttl_seconds)))
    rlock = RLock(cache, f"{lock_key}:reentrant", expire=max(1, int(ttl_seconds)))
    semaphore.acquire()
    lock.acquire()
    rlock.acquire()
    try:
        yield
    finally:
        with suppress(RuntimeError, TypeError, ValueError):
            rlock.release()
        with suppress(RuntimeError, TypeError, ValueError):
            lock.release()
        with suppress(RuntimeError, TypeError, ValueError):
            semaphore.release()


def publish_once_per_barrier(
    *,
    backend: CqCacheBackend,
    barrier_key: str,
    publish_fn: Callable[[], None],
) -> None:
    """Run publish function under diskcache `barrier` coordination when available."""
    cache = _fanout_cache(backend)
    if cache is None or barrier is None:
        publish_fn()
        return
    wrapped = barrier(cache, Lock, name=barrier_key)(publish_fn)
    wrapped()


__all__ = [
    "publish_once_per_barrier",
    "tree_sitter_lane_guard",
]
