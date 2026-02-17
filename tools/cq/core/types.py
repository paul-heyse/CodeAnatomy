"""Core shared enum/types for CQ modules."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal, cast

QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]

DEFAULT_QUERY_LANGUAGE: QueryLanguage = "python"
DEFAULT_QUERY_LANGUAGE_SCOPE: QueryLanguageScope = "auto"
SUPPORTED_QUERY_LANGUAGES: tuple[QueryLanguage, ...] = ("python", "rust")
SUPPORTED_QUERY_LANGUAGE_SCOPES: tuple[QueryLanguageScope, ...] = ("auto", "python", "rust")


def parse_query_language(value: str) -> QueryLanguage:
    """Parse and validate a concrete query language token.

    Raises:
        ValueError: If ``value`` is not a supported concrete language.

    Returns:
        QueryLanguage: Normalized concrete query language.
    """
    normalized = value.strip().lower()
    if normalized not in SUPPORTED_QUERY_LANGUAGES:
        msg = (
            f"Invalid query language: {value!r}. "
            f"Valid languages: {', '.join(SUPPORTED_QUERY_LANGUAGES)}"
        )
        raise ValueError(msg)
    return cast("QueryLanguage", normalized)


def parse_query_language_scope(value: str | None) -> QueryLanguageScope:
    """Parse and validate a query language scope token.

    Raises:
        ValueError: If ``value`` is not a supported language scope token.

    Returns:
        QueryLanguageScope: Normalized query language scope.
    """
    if value is None:
        return DEFAULT_QUERY_LANGUAGE_SCOPE
    normalized = value.strip().lower()
    if normalized not in SUPPORTED_QUERY_LANGUAGE_SCOPES:
        msg = (
            f"Invalid query language scope: {value!r}. "
            f"Valid scopes: {', '.join(SUPPORTED_QUERY_LANGUAGE_SCOPES)}"
        )
        raise ValueError(msg)
    return cast("QueryLanguageScope", normalized)


def is_python_language(lang: QueryLanguage | str) -> bool:
    """Return whether a language token resolves to Python."""
    return str(lang).strip().lower() == "python"


def is_rust_language(lang: QueryLanguage | str) -> bool:
    """Return whether a language token resolves to Rust."""
    return str(lang).strip().lower() == "rust"


class LdmdSliceMode(StrEnum):
    """LDMD extraction mode token."""

    full = "full"
    preview = "preview"
    tldr = "tldr"

    def __str__(self) -> str:
        """Return the enum wire value."""
        return self.value


__all__ = [
    "DEFAULT_QUERY_LANGUAGE",
    "DEFAULT_QUERY_LANGUAGE_SCOPE",
    "SUPPORTED_QUERY_LANGUAGES",
    "SUPPORTED_QUERY_LANGUAGE_SCOPES",
    "LdmdSliceMode",
    "QueryLanguage",
    "QueryLanguageScope",
    "is_python_language",
    "is_rust_language",
    "parse_query_language",
    "parse_query_language_scope",
]
