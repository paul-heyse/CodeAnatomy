"""Shared helpers for cache-related env/config resolution."""

from __future__ import annotations

import os


def env_flag(name: str, *, default: bool = False) -> bool:
    """Parse a boolean feature flag from environment variables.

    Returns:
        bool: Parsed flag value or default when missing/invalid.
    """
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def env_positive_int(name: str, *, default: int) -> int:
    """Parse a positive integer from environment variables with fallback.

    Returns:
        int: Parsed positive integer or fallback default.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(raw.strip())
    except ValueError:
        return default
    return parsed if parsed > 0 else default


__all__ = ["env_flag", "env_positive_int"]
