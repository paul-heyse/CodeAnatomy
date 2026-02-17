"""Disk-backed CQ cache backend built on diskcache FanoutCache."""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Final

from diskcache import FanoutCache, Timeout

from tools.cq.core.cache.cache_runtime_tuning import (
    apply_cache_runtime_tuning,
    resolve_cache_runtime_tuning,
)
from tools.cq.core.cache.interface import CqCacheBackend, NoopCacheBackend
from tools.cq.core.cache.policy import CqCachePolicyV1
from tools.cq.core.cache.telemetry import (
    record_cache_abort,
    record_cache_cull,
    record_cache_timeout,
    record_cache_volume,
)

_NON_FATAL_ERRORS: Final = (
    OSError,
    RuntimeError,
    ValueError,
    TypeError,
    sqlite3.DatabaseError,
)
_OPEN_ERRORS: Final = (Timeout, *_NON_FATAL_ERRORS)
_NAMESPACE_PARTS_MIN_LENGTH: Final = 2
_NAMESPACE_PREFIX_LENGTH: Final = 3


class _FailOpenTransaction:
    """Fail-open transaction wrapper that degrades to no-op on backend errors."""

    def __init__(self, backend: DiskcacheBackend) -> None:
        self.backend = backend
        self.ctx: object | None = None

    def __enter__(self) -> None:
        try:
            ctx = self.backend.cache.transact(retry=True)
            self.ctx = ctx
            enter = getattr(ctx, "__enter__", None)
            if callable(enter):
                enter()
        except Timeout:
            self.backend.record_timeout(namespace="transaction")
            self.ctx = None
        except _NON_FATAL_ERRORS:
            self.backend.record_abort(namespace="transaction")
            self.ctx = None

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        if self.ctx is None:
            return False
        exit_fn = getattr(self.ctx, "__exit__", None)
        if not callable(exit_fn):
            return False
        try:
            return bool(exit_fn(exc_type, exc, tb))
        except Timeout:
            self.backend.record_timeout(namespace="transaction")
            return False
        except _NON_FATAL_ERRORS:
            self.backend.record_abort(namespace="transaction")
            return False


class _FailOpenCoordinationContext:
    """Fail-open coordination wrapper for lock/semaphore primitives."""

    def __init__(self, backend: DiskcacheBackend, *, ctx: object, namespace: str) -> None:
        self.backend = backend
        self.ctx = ctx
        self.namespace = namespace
        self.active = False

    def __enter__(self) -> None:
        enter = getattr(self.ctx, "__enter__", None)
        if not callable(enter):
            return
        try:
            enter()
            self.active = True
        except Timeout:
            self.backend.record_timeout(namespace=self.namespace)
            self.active = False
        except _NON_FATAL_ERRORS:
            self.backend.record_abort(namespace=self.namespace)
            self.active = False

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        if not self.active:
            return False
        exit_fn = getattr(self.ctx, "__exit__", None)
        if not callable(exit_fn):
            return False
        try:
            return bool(exit_fn(exc_type, exc, tb))
        except Timeout:
            self.backend.record_timeout(namespace=self.namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.backend.record_abort(namespace=self.namespace)
            return False


class DiskcacheBackend:
    """Fail-open diskcache backend for CQ runtime."""

    def __init__(
        self,
        cache: FanoutCache,
        *,
        default_ttl_seconds: int,
        transaction_batch_size: int = 128,
    ) -> None:
        """Initialize disk cache adapter.

        Args:
            cache: Backing diskcache instance.
            default_ttl_seconds: Default cache TTL in seconds.
            transaction_batch_size: Max keys to write per transaction batch.
        """
        self.cache = cache
        self.default_ttl_seconds = default_ttl_seconds
        self.transaction_batch_size = max(1, int(transaction_batch_size))

    @staticmethod
    def _namespace_from_key(key: str) -> str:
        if key.startswith("cq:"):
            parts = key.split(":", maxsplit=3)
            if len(parts) >= _NAMESPACE_PARTS_MIN_LENGTH and parts[1]:
                return parts[1]
        return "cache_backend"

    @staticmethod
    def _namespace_from_tag(tag: str) -> str:
        for atom in tag.split("|"):
            if atom.startswith("ns:") and len(atom) > _NAMESPACE_PREFIX_LENGTH:
                return atom[3:]
        return "cache_backend"

    @staticmethod
    def record_timeout(namespace: str) -> None:
        """Record cache timeout event for ``namespace``."""
        record_cache_timeout(namespace=namespace)

    @staticmethod
    def record_abort(namespace: str) -> None:
        """Record non-timeout cache abort for ``namespace``."""
        record_cache_abort(namespace=namespace)

    def get(self, key: str) -> object | None:
        """Fetch key from cache.

        Returns:
            object | None: Cached value for the key, or `None` on miss/error.
        """
        namespace = self._namespace_from_key(key)
        try:
            return self.cache.get(key, default=None, retry=True)
        except Timeout:
            self.record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return None

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Write value to cache and return acknowledgement.

        Returns:
            bool: `True` when the value was written, `False` otherwise.
        """
        ttl = expire if expire is not None else self.default_ttl_seconds
        namespace = self._namespace_from_key(key)
        try:
            return bool(self.cache.set(key, value, expire=ttl, tag=tag, retry=True))
        except Timeout:
            self.record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return False

    def add(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Write value only when absent and return acknowledgement.

        Returns:
            bool: `True` when the value was added, `False` otherwise.
        """
        ttl = expire if expire is not None else self.default_ttl_seconds
        namespace = self._namespace_from_key(key)
        try:
            return bool(self.cache.add(key, value, expire=ttl, tag=tag, retry=True))
        except Timeout:
            self.record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return False

    def set_many(
        self,
        items: dict[str, object],
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> int:
        """Write multiple keys in one best-effort transaction.

        Returns:
            int: Number of keys successfully written.
        """
        written = 0
        if not items:
            return written
        rows = list(items.items())
        batch_size = max(1, int(self.transaction_batch_size))
        for start in range(0, len(rows), batch_size):
            with self.transact():
                for key, value in rows[start : start + batch_size]:
                    if self.set(key, value, expire=expire, tag=tag):
                        written += 1
        return written

    def incr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Increment numeric value and return updated value.

        Returns:
            int | None: Updated integer value when present, otherwise `None`.
        """
        namespace = self._namespace_from_key(key)
        try:
            value = self.cache.incr(key, delta=delta, default=default, retry=True)
            if value is None:
                return None
            return int(value)
        except Timeout:
            self.record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return None

    def decr(self, key: str, delta: int = 1, default: int = 0) -> int | None:
        """Decrement numeric value and return updated value.

        Returns:
            int | None: Updated integer value when present, otherwise `None`.
        """
        namespace = self._namespace_from_key(key)
        try:
            value = self.cache.decr(key, delta=delta, default=default, retry=True)
            if value is None:
                return None
            return int(value)
        except Timeout:
            self.record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return None

    def delete(self, key: str) -> bool:
        """Delete key from cache and return acknowledgement.

        Returns:
            bool: `True` when deletion was attempted successfully, `False` otherwise.
        """
        namespace = self._namespace_from_key(key)
        try:
            return bool(self.cache.delete(key, retry=True))
        except Timeout:
            self.record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return False

    def evict_tag(self, tag: str) -> bool:
        """Evict tagged items and return acknowledgement.

        Returns:
            bool: `True` when tag eviction succeeded, `False` otherwise.
        """
        namespace = self._namespace_from_tag(tag)
        try:
            self.cache.evict(tag, retry=True)
        except Timeout:
            self.record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return False
        else:
            return True

    def transact(self) -> _FailOpenTransaction:
        """Return fail-open transaction context manager.

        Returns:
            _FailOpenTransaction: Context manager for best-effort transactional scope.
        """
        return _FailOpenTransaction(self)

    def stats(self) -> dict[str, object]:
        """Return cache stats payload.

        Returns:
            dict[str, object]: Cache hit/miss counts and related metrics.
        """
        try:
            hits, misses = self.cache.stats(enable=False, reset=False)
            return {
                "hits": int(hits),
                "misses": int(misses),
            }
        except Timeout:
            self.record_timeout(namespace="cache_backend")
            return {}
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace="cache_backend")
            return {}

    def volume(self) -> int | None:
        """Return backend volume in bytes, when available.

        Returns:
            int | None: Estimated cache volume in bytes, or `None` on error.
        """
        try:
            volume_bytes = int(self.cache.volume())
            record_cache_volume(namespace="cache_backend", volume_bytes=volume_bytes)
        except Timeout:
            self.record_timeout(namespace="cache_backend")
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace="cache_backend")
            return None
        else:
            return volume_bytes

    def cull(self) -> int | None:
        """Trigger backend cull and return number of removed entries.

        Returns:
            int | None: Number of entries removed, or `None` on error.
        """
        try:
            removed = int(self.cache.cull(retry=True))
            record_cache_cull(namespace="cache_backend", removed=removed)
        except Timeout:
            self.record_timeout(namespace="cache_backend")
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace="cache_backend")
            return None
        else:
            return removed

    def touch(self, key: str, *, expire: int | None = None) -> bool:
        """Refresh key TTL and return acknowledgement.

        Returns:
            bool: ``True`` when the key TTL was refreshed, ``False`` otherwise.
        """
        namespace = self._namespace_from_key(key)
        ttl = expire if expire is not None else self.default_ttl_seconds
        try:
            return bool(self.cache.touch(key, expire=ttl, retry=True))
        except Timeout:
            self.record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return False

    def expire(self) -> int | None:
        """Run expiry sweep and return removed row count.

        Returns:
            int | None: Number of entries removed, or ``None`` on non-fatal error.
        """
        try:
            return int(self.cache.expire(retry=True))
        except Timeout:
            self.record_timeout(namespace="cache_backend")
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace="cache_backend")
            return None

    def check(self, *, fix: bool = False) -> int | None:
        """Run cache integrity checks and return error count.

        Returns:
            int | None: Number of integrity errors detected, or ``None`` on
                non-fatal error.
        """
        try:
            return int(self.cache.check(fix=fix, retry=True))
        except Timeout:
            self.record_timeout(namespace="cache_backend")
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace="cache_backend")
            return None

    def close(self) -> None:
        """Close cache resources."""
        try:
            self.cache.close()
        except _NON_FATAL_ERRORS:
            return

    def read_streaming(self, key: str) -> bytes | None:
        """Read byte payload via diskcache streaming API when available.

        Returns:
            bytes | None: Streamed payload when present.
        """
        namespace = self._namespace_from_key(key)
        read_fn = getattr(self.cache, "read", None)
        if not callable(read_fn):
            value = self.get(key)
            if isinstance(value, (bytes, bytearray, memoryview)):
                return bytes(value)
            return None
        try:
            reader_cm = read_fn(key, retry=True)
        except TypeError:
            try:
                reader_cm = read_fn(key)
            except Timeout:
                self.record_timeout(namespace=namespace)
                return None
            except _NON_FATAL_ERRORS:
                self.record_abort(namespace=namespace)
                return None
        except Timeout:
            self.record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return None
        try:
            with reader_cm as reader:
                read_method = getattr(reader, "read", None)
                if not callable(read_method):
                    return None
                payload = read_method()
        except Timeout:
            self.record_timeout(namespace=namespace)
            return None
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return None
        if isinstance(payload, memoryview):
            return payload.tobytes()
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
        return None

    def set_streaming(
        self,
        key: str,
        payload: bytes,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool:
        """Write byte payload via diskcache read=True API when available.

        Returns:
            bool: ``True`` when payload is written.
        """
        from tempfile import NamedTemporaryFile

        namespace = self._namespace_from_key(key)
        ttl = expire if expire is not None else self.default_ttl_seconds
        try:
            with NamedTemporaryFile("w+b", delete=True) as tmp:
                tmp.write(payload)
                tmp.flush()
                tmp.seek(0)
                try:
                    return bool(
                        self.cache.set(
                            key,
                            tmp,
                            read=True,
                            expire=ttl,
                            tag=tag,
                            retry=True,
                        )
                    )
                except TypeError:
                    tmp.seek(0)
                    return bool(
                        self.cache.set(
                            key,
                            tmp,
                            read=True,
                            expire=ttl,
                            tag=tag,
                        )
                    )
        except Timeout:
            self.record_timeout(namespace=namespace)
            return False
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=namespace)
            return False

    @contextmanager
    def lock(self, key: str, *, expire: int | None = None) -> Iterator[None]:
        """Return fail-open lock context for key."""
        try:
            from diskcache import Lock
        except ImportError:
            with nullcontext():
                yield
            return
        ttl = expire if expire is not None else self.default_ttl_seconds
        ctx = _FailOpenCoordinationContext(
            self,
            ctx=Lock(self.cache, key, expire=ttl),
            namespace=self._namespace_from_key(key),
        )
        with ctx:
            yield

    @contextmanager
    def rlock(self, key: str, *, expire: int | None = None) -> Iterator[None]:
        """Return fail-open reentrant-lock context for key."""
        try:
            from diskcache import RLock
        except ImportError:
            with nullcontext():
                yield
            return
        ttl = expire if expire is not None else self.default_ttl_seconds
        ctx = _FailOpenCoordinationContext(
            self,
            ctx=RLock(self.cache, key, expire=ttl),
            namespace=self._namespace_from_key(key),
        )
        with ctx:
            yield

    @contextmanager
    def semaphore(
        self,
        key: str,
        *,
        value: int,
        expire: int | None = None,
    ) -> Iterator[None]:
        """Return fail-open semaphore context for key."""
        try:
            from diskcache import BoundedSemaphore
        except ImportError:
            with nullcontext():
                yield
            return
        ttl = expire if expire is not None else self.default_ttl_seconds
        ctx = _FailOpenCoordinationContext(
            self,
            ctx=BoundedSemaphore(self.cache, key, value=max(1, int(value)), expire=ttl),
            namespace=self._namespace_from_key(key),
        )
        with ctx:
            yield

    def barrier(self, key: str, publish_fn: Callable[[], None]) -> None:
        """Run publish function once with diskcache barrier when available."""
        try:
            from diskcache import Lock, barrier
        except ImportError:
            publish_fn()
            return
        try:
            wrapped = barrier(self.cache, Lock, name=key)(publish_fn)
            wrapped()
        except Timeout:
            self.record_timeout(namespace=self._namespace_from_key(key))
            publish_fn()
        except _NON_FATAL_ERRORS:
            self.record_abort(namespace=self._namespace_from_key(key))
            publish_fn()


def _build_diskcache_backend(policy: CqCachePolicyV1) -> CqCacheBackend:
    directory = Path(policy.directory).expanduser()

    def _open_cache() -> FanoutCache:
        return FanoutCache(
            directory=str(directory),
            shards=max(1, int(policy.shards)),
            timeout=float(policy.timeout_seconds),
            tag_index=True,
            statistics=bool(policy.statistics_enabled),
            size_limit=max(1, int(policy.size_limit_bytes)),
            cull_limit=max(0, int(policy.cull_limit)),
            eviction_policy=policy.eviction_policy,
        )

    try:
        cache = _open_cache()
    except _OPEN_ERRORS:
        try:
            shutil.rmtree(directory, ignore_errors=True)
        except OSError:
            return NoopCacheBackend()
        try:
            cache = _open_cache()
        except _OPEN_ERRORS:
            return NoopCacheBackend()
    tuning = resolve_cache_runtime_tuning(policy)
    apply_cache_runtime_tuning(cache, tuning)
    return DiskcacheBackend(
        cache,
        default_ttl_seconds=policy.ttl_seconds,
        transaction_batch_size=tuning.transaction_batch_size,
    )


from tools.cq.core.cache.backend_lifecycle import (
    close_cq_cache_backend,
    get_cq_cache_backend,
    set_cq_cache_backend,
)

__all__ = [
    "DiskcacheBackend",
    "close_cq_cache_backend",
    "get_cq_cache_backend",
    "set_cq_cache_backend",
]
