"""Cache telemetry helpers for Delta-backed cache operations."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass

from opentelemetry.trace import Span

from obs.otel.attributes import normalize_attributes
from obs.otel.metrics import record_cache_event
from obs.otel.run_context import get_run_id
from obs.otel.scopes import SCOPE_DATAFUSION
from obs.otel.tracing import get_tracer, record_exception, set_span_attributes, span_attributes

CacheResultSetter = Callable[[str], None]


@dataclass
class CacheRunStats:
    """In-memory cache run statistics for run-level summaries."""

    run_id: str
    start_time_unix_ms: int
    end_time_unix_ms: int
    read_count: int = 0
    write_count: int = 0
    snapshot_count: int = 0
    error_count: int = 0
    hit_count: int = 0
    miss_count: int = 0

    def record(self, *, operation: str, result: str, event_time_unix_ms: int) -> None:
        """Update counters for a cache operation."""
        self.end_time_unix_ms = event_time_unix_ms
        if operation == "read":
            self.read_count += 1
            if result == "hit":
                self.hit_count += 1
            elif result == "miss":
                self.miss_count += 1
        elif operation == "write":
            self.write_count += 1
        elif operation == "snapshot":
            self.snapshot_count += 1
        if result == "error":
            self.error_count += 1


_CACHE_RUN_STATS: dict[str, CacheRunStats] = {}
_CACHE_RUN_LOCK = threading.Lock()


def _base_cache_attributes(
    *,
    cache_policy: str,
    cache_scope: str,
    operation: str,
    attributes: Mapping[str, object] | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "cache.policy": cache_policy,
        "cache.scope": cache_scope,
        "cache.operation": operation,
    }
    if attributes:
        payload.update(attributes)
    return payload


def _record_cache_run_stats(*, operation: str, result: str) -> None:
    run_id = get_run_id()
    if run_id is None:
        return
    event_time_unix_ms = int(time.time() * 1000)
    with _CACHE_RUN_LOCK:
        stats = _CACHE_RUN_STATS.get(run_id)
        if stats is None:
            stats = CacheRunStats(
                run_id=run_id,
                start_time_unix_ms=event_time_unix_ms,
                end_time_unix_ms=event_time_unix_ms,
            )
            _CACHE_RUN_STATS[run_id] = stats
        stats.record(
            operation=operation,
            result=result,
            event_time_unix_ms=event_time_unix_ms,
        )


def drain_cache_run_stats(run_id: str) -> CacheRunStats | None:
    """Return and clear cached run stats for a run id.

    Returns:
    -------
    CacheRunStats | None
        Cached run stats payload when available.
    """
    with _CACHE_RUN_LOCK:
        return _CACHE_RUN_STATS.pop(run_id, None)


@contextmanager
def cache_span(
    name: str,
    *,
    cache_policy: str,
    cache_scope: str,
    operation: str,
    attributes: Mapping[str, object] | None = None,
) -> Iterator[tuple[Span, CacheResultSetter]]:
    """Create a cache span and record cache metrics.

    Parameters
    ----------
    name
        Span name.
    cache_policy
        Cache policy identifier.
    cache_scope
        Cache scope (view/dataset/artifact/metadata).
    operation
        Cache operation (read/write/snapshot/inventory).

    Attributes:
        Optional span attributes.

    Yields:
    ------
    tuple[Span, CacheResultSetter]
        The active span and a setter for cache result.
    """
    base_attrs = _base_cache_attributes(
        cache_policy=cache_policy,
        cache_scope=cache_scope,
        operation=operation,
        attributes=attributes,
    )
    tracer = get_tracer(SCOPE_DATAFUSION)
    start = time.monotonic()
    result = "ok"

    def set_result(value: str) -> None:
        nonlocal result
        result = value

    with tracer.start_as_current_span(name, attributes=span_attributes(attrs=base_attrs)) as span:
        span.add_event(
            "cache.start",
            attributes=normalize_attributes(
                {
                    "cache.policy": cache_policy,
                    "cache.scope": cache_scope,
                    "cache.operation": operation,
                }
            ),
        )
        try:
            yield span, set_result
        except Exception as exc:
            result = "error"
            record_exception(span, exc)
            raise
        finally:
            duration_s = time.monotonic() - start
            set_span_attributes(
                span,
                {
                    "cache.result": result,
                    "duration_s": duration_s,
                },
            )
            record_cache_event(
                cache_policy=cache_policy,
                cache_scope=cache_scope,
                operation=operation,
                result=result,
                duration_s=duration_s,
            )
            _record_cache_run_stats(operation=operation, result=result)
            span.add_event(
                "cache.end",
                attributes=normalize_attributes(
                    {
                        "cache.result": result,
                        "duration_s": duration_s,
                    }
                ),
            )


__all__ = ["CacheRunStats", "cache_span", "drain_cache_run_stats"]
