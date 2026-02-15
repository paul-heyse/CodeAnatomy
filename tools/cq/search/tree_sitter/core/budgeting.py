"""Budget and cancellation helpers for tree-sitter execution lanes."""

from __future__ import annotations

from time import monotonic


def budget_ms_per_anchor(
    *,
    timeout_seconds: float,
    max_anchors: int,
    reserve_fraction: float = 0.5,
    min_budget_ms: int = 25,
    max_budget_ms: int = 2_000,
) -> int:
    """Derive a bounded per-anchor query budget from request timeout bounds.

    Returns:
        int: Per-anchor budget in milliseconds.
    """
    safe_timeout = max(0.1, float(timeout_seconds))
    safe_anchors = max(1, int(max_anchors))
    usable_seconds = safe_timeout * max(0.05, min(0.95, reserve_fraction))
    budget = int((usable_seconds * 1000.0) / safe_anchors)
    return max(min_budget_ms, min(max_budget_ms, budget))


def deadline_from_budget_ms(budget_ms: int | None) -> float | None:
    """Return a monotonic deadline, or ``None`` when no budget is enforced."""
    if budget_ms is None or budget_ms <= 0:
        return None
    return monotonic() + (float(budget_ms) / 1000.0)


def is_deadline_expired(deadline: float | None) -> bool:
    """Return whether a monotonic deadline has elapsed."""
    if deadline is None:
        return False
    return monotonic() >= deadline


__all__ = ["budget_ms_per_anchor", "deadline_from_budget_ms", "is_deadline_expired"]
