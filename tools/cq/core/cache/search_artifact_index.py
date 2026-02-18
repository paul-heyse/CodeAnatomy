"""Path and backend helpers for search artifact index/deque stores."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Protocol, cast

from tools.cq.core.cache.policy import CqCachePolicyV1

_NAMESPACE = "search_artifacts"
_MAX_INDEX_ROWS = 1000

try:
    from diskcache import Deque, Index
except ImportError:  # pragma: no cover - optional dependency
    Deque = None
    Index = None


class SearchArtifactIndexLike(Protocol):
    """Protocol surface for diskcache index store operations."""

    def __setitem__(self, key: str, value: object) -> None:
        """Store one artifact row by cache key."""
        ...

    def get(self, key: str, default: object | None = None) -> object | None:
        """Return cached artifact row for key, or default when absent."""
        ...

    def values(self) -> Iterable[object]:
        """Iterate cached artifact rows."""
        ...


class SearchArtifactDequeLike(Protocol):
    """Protocol surface for diskcache deque store operations."""

    def appendleft(self, item: str) -> None:
        """Prepend one cache key to the artifact order deque."""
        ...

    def __iter__(self) -> Iterator[str]:
        """Iterate cache keys in deque order."""
        ...


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


def open_artifact_deque(path: Path) -> SearchArtifactDequeLike | None:
    """Open artifact deque store when diskcache is available.

    Returns:
        Opened deque handle, or `None` when diskcache is unavailable.
    """
    if Deque is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    return cast("SearchArtifactDequeLike", Deque(str(path), maxlen=_MAX_INDEX_ROWS))


def open_artifact_index(path: Path) -> SearchArtifactIndexLike | None:
    """Open artifact index store when diskcache is available.

    Returns:
        Opened index handle, or `None` when diskcache is unavailable.
    """
    if Index is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    return cast("SearchArtifactIndexLike", Index(str(path)))


__all__ = [
    "SearchArtifactDequeLike",
    "SearchArtifactIndexLike",
    "global_index_path",
    "global_order_path",
    "open_artifact_deque",
    "open_artifact_index",
    "run_index_path",
    "run_order_path",
    "store_root",
]
