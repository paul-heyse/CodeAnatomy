"""Write-planning primitives for DataFusion write operations."""

from __future__ import annotations

from collections.abc import Sequence


def resolved_partition_by(partition_by: Sequence[str] | None) -> tuple[str, ...]:
    """Return normalized partition columns for write planning."""
    return tuple(str(name) for name in (partition_by or ()))


__all__ = ["resolved_partition_by"]
