"""Core shared enum/types for CQ modules."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]


class LdmdSliceMode(StrEnum):
    """LDMD extraction mode token."""

    full = "full"
    preview = "preview"
    tldr = "tldr"

    def __str__(self) -> str:
        """Return the enum wire value."""
        return self.value


__all__ = ["LdmdSliceMode", "QueryLanguage", "QueryLanguageScope"]
