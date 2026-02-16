"""Validation helpers for write requests."""

from __future__ import annotations


def validate_destination(destination: str) -> None:
    """Validate non-empty write destination.

    Raises:
        ValueError: If destination is empty or whitespace.
    """
    if not destination.strip():
        msg = "Write destination must be non-empty."
        raise ValueError(msg)


__all__ = ["validate_destination"]
