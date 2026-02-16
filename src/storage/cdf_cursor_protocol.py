"""Shared cursor-store protocols for Delta CDF state."""

from __future__ import annotations

from typing import Protocol


class CdfCursorLike(Protocol):
    """Structural contract for persisted CDF cursor records."""

    dataset_name: str
    last_version: int
    last_timestamp: str | None


class CdfCursorStoreLike(Protocol):
    """Structural contract for CDF cursor stores."""

    def load_cursor(self, dataset_name: str) -> CdfCursorLike | None:
        """Load cursor state for ``dataset_name`` if one exists."""
        ...

    def save_cursor(self, cursor: CdfCursorLike) -> None:
        """Persist cursor state for a dataset."""
        ...


__all__ = ["CdfCursorLike", "CdfCursorStoreLike"]
