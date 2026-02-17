"""Adaptive runtime helpers backed by cache protocol primitives."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
from tools.cq.search.tree_sitter.contracts.core_models import AdaptiveRuntimeSnapshotV1

_RUNTIME_TTL_SECONDS = 300


def _stats_key(language: str) -> str:
    return f"tree_sitter:runtime_stats:{language}"


def _resolve_backend(cache_backend: object | None = None) -> object:
    if cache_backend is not None:
        return cache_backend
    return get_cq_cache_backend(root=Path.cwd())


def memoized_value[T](
    *,
    key: str,
    compute: Callable[[], T],
    ttl_seconds: int = _RUNTIME_TTL_SECONDS,
    cache_backend: object | None = None,
) -> T:
    """Resolve cached value using cache protocol get/set.

    Returns:
        T: Cached value when present, otherwise computed value.
    """
    backend = _resolve_backend(cache_backend)
    get_fn = getattr(backend, "get", None)
    set_fn = getattr(backend, "set", None)
    if not callable(get_fn) or not callable(set_fn):
        return compute()
    hit = get_fn(key)
    if hit is not None:
        return cast("T", hit)
    value = compute()
    set_fn(key, value, expire=ttl_seconds)
    return value


def _read_runtime_stats(language: str, *, cache_backend: object | None = None) -> dict[str, object]:
    backend = _resolve_backend(cache_backend)
    get_fn = getattr(backend, "get", None)
    if not callable(get_fn):
        return {"count": 0, "sum_ms": 0.0}
    raw = get_fn(_stats_key(language))
    if not isinstance(raw, dict):
        return {"count": 0, "sum_ms": 0.0}
    return raw


def _write_runtime_stats(
    language: str,
    stats: dict[str, object],
    *,
    cache_backend: object | None = None,
) -> None:
    backend = _resolve_backend(cache_backend)
    set_fn = getattr(backend, "set", None)
    if not callable(set_fn):
        return
    set_fn(
        _stats_key(language),
        stats,
        expire=_RUNTIME_TTL_SECONDS,
        tag="ns:tree_sitter|kind:runtime_ms",
    )


def record_runtime_sample(
    language: str,
    elapsed_ms: float,
    *,
    cache_backend: object | None = None,
) -> None:
    """Record per-language latency sample for adaptive budgeting."""
    if elapsed_ms <= 0:
        return
    stats = _read_runtime_stats(language, cache_backend=cache_backend)
    count = stats.get("count")
    sum_ms = stats.get("sum_ms")
    next_count = int(count) + 1 if isinstance(count, int) else 1
    next_sum = (
        float(sum_ms) + float(elapsed_ms) if isinstance(sum_ms, int | float) else float(elapsed_ms)
    )
    _write_runtime_stats(
        language,
        {"count": next_count, "sum_ms": next_sum},
        cache_backend=cache_backend,
    )


def _cached_average_ms(language: str, *, cache_backend: object | None = None) -> float | None:
    stats = _read_runtime_stats(language, cache_backend=cache_backend)
    count = stats.get("count")
    sum_ms = stats.get("sum_ms")
    if not isinstance(count, int) or count <= 0:
        return None
    if not isinstance(sum_ms, int | float):
        return None
    return float(sum_ms) / float(count)


def _cached_sample_count(language: str, *, cache_backend: object | None = None) -> int | None:
    stats = _read_runtime_stats(language, cache_backend=cache_backend)
    count = stats.get("count")
    if isinstance(count, int):
        return count
    return None


def adaptive_query_budget_ms(
    *,
    language: str,
    fallback_budget_ms: int,
    cache_backend: object | None = None,
) -> int:
    """Return adaptive query budget derived from recent runtime latency."""
    cached_average = _cached_average_ms(language, cache_backend=cache_backend)
    if cached_average is not None:
        bounded_budget = max(25.0, min(2_500.0, cached_average * 4.0))
        return int(bounded_budget)
    return fallback_budget_ms


def runtime_snapshot(
    language: str,
    *,
    fallback_budget_ms: int,
    cache_backend: object | None = None,
) -> AdaptiveRuntimeSnapshotV1:
    """Return current adaptive runtime snapshot for one language lane."""
    cached_average = _cached_average_ms(language, cache_backend=cache_backend)
    cached_count = _cached_sample_count(language, cache_backend=cache_backend)
    if cached_average is None:
        cached_average = 0.0
    if cached_count is None:
        cached_count = 0
    recommended = adaptive_query_budget_ms(
        language=language,
        fallback_budget_ms=fallback_budget_ms,
        cache_backend=cache_backend,
    )
    return AdaptiveRuntimeSnapshotV1(
        language=language,
        average_latency_ms=float(cached_average),
        sample_count=max(0, int(cached_count)),
        recommended_budget_ms=recommended,
    )


def derive_degrade_reason(
    *,
    exceeded_match_limit: bool,
    cancelled: bool,
    containment_required: bool = False,
    window_applied: bool = True,
) -> str | None:
    """Return deterministic degrade reason for query runtime telemetry."""
    if containment_required and not window_applied:
        return "containment_api_unavailable"
    if exceeded_match_limit:
        return "match_limit_exceeded"
    if cancelled:
        return "budget_cancelled"
    return None


__all__ = [
    "adaptive_query_budget_ms",
    "derive_degrade_reason",
    "memoized_value",
    "record_runtime_sample",
    "runtime_snapshot",
]
