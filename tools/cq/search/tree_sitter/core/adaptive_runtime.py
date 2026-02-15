"""Adaptive runtime helpers backed by diskcache-aware primitives."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import cast

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.search.tree_sitter.contracts.core_models import AdaptiveRuntimeSnapshotV1

try:
    from diskcache import Averager
except ImportError:  # pragma: no cover - optional dependency
    Averager = None


def _cache() -> object | None:
    backend = get_cq_cache_backend(root=Path.cwd())
    return getattr(backend, "cache", None)


def _sample_count_key(language: str) -> str:
    return f"tree_sitter:runtime_samples:{language}:count"


def _averager(language: str) -> object | None:
    cache = _cache()
    if cache is None or Averager is None:
        return None
    try:
        return Averager(
            cache,
            f"tree_sitter:runtime_ms:{language}",
            expire=300,
            tag="ns:tree_sitter|kind:runtime_ms",
        )
    except (RuntimeError, TypeError, ValueError):
        return None


def memoized_value[T](
    *,
    key: str,
    compute: Callable[[], T],
    ttl_seconds: int = 300,
) -> T:
    """Resolve cached value using diskcache memoize/get-set fallback.

    Returns:
        T: Cached or computed value.
    """
    cache = _cache()
    if cache is None:
        return compute()
    get_fn = getattr(cache, "get", None)
    set_fn = getattr(cache, "set", None)
    if callable(get_fn) and callable(set_fn):
        hit = get_fn(key)
        if hit is not None:
            return cast("T", hit)
        value = compute()
        set_fn(key, value, expire=ttl_seconds)
        return value
    return compute()


def record_runtime_sample(language: str, elapsed_ms: float) -> None:
    """Record per-language latency sample for adaptive budgeting."""
    if elapsed_ms <= 0:
        return
    sample = float(elapsed_ms)
    avg = _averager(language)
    if avg is None:
        return
    add_fn = getattr(avg, "add", None)
    if callable(add_fn):
        with suppress(RuntimeError, TypeError, ValueError):
            add_fn(sample)
    cache = _cache()
    if cache is None:
        return
    incr_fn = getattr(cache, "incr", None)
    if not callable(incr_fn):
        return
    try:
        incr_fn(_sample_count_key(language), delta=1, default=0)
    except TypeError:
        with suppress(RuntimeError, ValueError, OSError, AttributeError):
            incr_fn(_sample_count_key(language), 1)
    except (RuntimeError, ValueError, OSError, AttributeError):
        pass


def _cached_average_ms(language: str) -> float | None:
    avg = _averager(language)
    if avg is None:
        return None
    get_fn = getattr(avg, "get", None)
    if not callable(get_fn):
        return None
    try:
        value = get_fn()
    except (RuntimeError, TypeError, ValueError):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _cached_sample_count(language: str) -> int | None:
    cache = _cache()
    if cache is None:
        return None
    get_fn = getattr(cache, "get", None)
    if not callable(get_fn):
        return None
    try:
        value = get_fn(_sample_count_key(language))
    except (RuntimeError, TypeError, ValueError, OSError, AttributeError):
        return None
    if isinstance(value, int):
        return value
    return None


def adaptive_query_budget_ms(*, language: str, fallback_budget_ms: int) -> int:
    """Return adaptive query budget derived from recent runtime latency."""
    cached_average = _cached_average_ms(language)
    if cached_average is not None:
        bounded_budget = max(25.0, min(2_500.0, cached_average * 4.0))
        return int(bounded_budget)
    return fallback_budget_ms


def runtime_snapshot(language: str, *, fallback_budget_ms: int) -> AdaptiveRuntimeSnapshotV1:
    """Return current adaptive runtime snapshot for one language lane."""
    cached_average = _cached_average_ms(language)
    cached_count = _cached_sample_count(language)
    if cached_average is None:
        cached_average = 0.0
    if cached_count is None:
        cached_count = 0
    recommended = adaptive_query_budget_ms(language=language, fallback_budget_ms=fallback_budget_ms)
    return AdaptiveRuntimeSnapshotV1(
        language=language,
        average_latency_ms=float(cached_average),
        sample_count=max(0, int(cached_count)),
        recommended_budget_ms=recommended,
    )


__all__ = [
    "adaptive_query_budget_ms",
    "memoized_value",
    "record_runtime_sample",
    "runtime_snapshot",
]
