"""Core shared enum/types for CQ modules."""

from __future__ import annotations

from enum import StrEnum


class LdmdSliceMode(StrEnum):
    """LDMD extraction mode token."""

    full = "full"
    preview = "preview"
    tldr = "tldr"

    def __str__(self) -> str:
        """Return the enum wire value."""
        return self.value


__all__ = ["LdmdSliceMode"]
