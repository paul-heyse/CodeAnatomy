"""Path helpers for search artifact index/deque stores."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.policy import CqCachePolicyV1

_NAMESPACE = "search_artifacts"


def store_root(policy: CqCachePolicyV1) -> Path:
    """Return root directory for search artifact stores."""
    return Path(policy.directory).expanduser() / "stores" / _NAMESPACE


def global_order_path(policy: CqCachePolicyV1) -> Path:
    """Return deque path for global artifact order."""
    return store_root(policy) / "deque" / "global_order"


def global_index_path(policy: CqCachePolicyV1) -> Path:
    """Return index path for global artifact index."""
    return store_root(policy) / "index" / "global"


def run_order_path(policy: CqCachePolicyV1, run_id: str) -> Path:
    """Return deque path for run-local artifact order."""
    return store_root(policy) / "deque" / f"run_{run_id}"


def run_index_path(policy: CqCachePolicyV1, run_id: str) -> Path:
    """Return index path for run-local artifact index."""
    return store_root(policy) / "index" / f"run_{run_id}"


__all__ = [
    "global_index_path",
    "global_order_path",
    "run_index_path",
    "run_order_path",
    "store_root",
]
