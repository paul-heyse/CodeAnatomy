"""Language helpers for CQ query and search execution."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Literal, cast

QueryLanguage = Literal["python", "rust"]
RipgrepLanguageType = Literal["py", "rust"]

DEFAULT_QUERY_LANGUAGE: QueryLanguage = "python"
SUPPORTED_QUERY_LANGUAGES: tuple[QueryLanguage, ...] = ("python", "rust")

RUST_QUERY_ENABLE_ENV = "CQ_ENABLE_RUST_QUERY"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})

LANGUAGE_FILE_EXTENSIONS: dict[QueryLanguage, tuple[str, ...]] = {
    "python": (".py",),
    "rust": (".rs",),
}
LANGUAGE_FILE_GLOBS: dict[QueryLanguage, list[str]] = {
    "python": ["*.py", "*.pyi"],
    "rust": ["*.rs"],
}
LANGUAGE_RIPGREP_TYPES: dict[QueryLanguage, RipgrepLanguageType] = {
    "python": "py",
    "rust": "rust",
}


def parse_query_language(value: str) -> QueryLanguage:
    """Parse and validate a query language token.

    Returns
    -------
    QueryLanguage
        Normalized query language token.

    Raises
    ------
    ValueError
        If the value does not map to a supported query language.
    """
    normalized = value.strip().lower()
    if normalized not in SUPPORTED_QUERY_LANGUAGES:
        msg = (
            f"Invalid query language: {value!r}. "
            f"Valid languages: {', '.join(SUPPORTED_QUERY_LANGUAGES)}"
        )
        raise ValueError(msg)
    return cast("QueryLanguage", normalized)


def file_extensions_for_language(lang: QueryLanguage) -> tuple[str, ...]:
    """Return supported file extensions for a query language.

    Returns
    -------
    tuple[str, ...]
        Supported source file extensions for the language.
    """
    return LANGUAGE_FILE_EXTENSIONS.get(lang, LANGUAGE_FILE_EXTENSIONS[DEFAULT_QUERY_LANGUAGE])


def file_globs_for_language(lang: QueryLanguage) -> list[str]:
    """Return summary glob patterns for a query language.

    Returns
    -------
    list[str]
        Glob patterns used for scanning files in the language.
    """
    return LANGUAGE_FILE_GLOBS.get(lang, LANGUAGE_FILE_GLOBS[DEFAULT_QUERY_LANGUAGE])


def ripgrep_type_for_language(lang: QueryLanguage) -> RipgrepLanguageType:
    """Return ripgrep file type token for a query language.

    Returns
    -------
    RipgrepLanguageType
        Ripgrep file type selector for the language.
    """
    return LANGUAGE_RIPGREP_TYPES.get(lang, LANGUAGE_RIPGREP_TYPES[DEFAULT_QUERY_LANGUAGE])


def is_query_language_enabled(
    lang: QueryLanguage,
    env: Mapping[str, str] | None = None,
) -> bool:
    """Return whether the query language is enabled in the current environment.

    Returns
    -------
    bool
        True when the language is enabled under current feature gates.
    """
    if lang != "rust":
        return True
    source_env = os.environ if env is None else env
    raw_value = source_env.get(RUST_QUERY_ENABLE_ENV, "")
    return raw_value.strip().lower() in _TRUE_VALUES


def disabled_language_message(lang: QueryLanguage) -> str:
    """Return a deterministic feature-gate message for a language.

    Returns
    -------
    str
        User-facing message describing how to enable the language.
    """
    if lang == "rust":
        return (
            "Rust queries are disabled. Set "
            f"{RUST_QUERY_ENABLE_ENV}=1 to enable Rust query execution."
        )
    return f"Language {lang!r} is disabled."


__all__ = [
    "DEFAULT_QUERY_LANGUAGE",
    "LANGUAGE_FILE_EXTENSIONS",
    "LANGUAGE_FILE_GLOBS",
    "LANGUAGE_RIPGREP_TYPES",
    "RUST_QUERY_ENABLE_ENV",
    "SUPPORTED_QUERY_LANGUAGES",
    "QueryLanguage",
    "RipgrepLanguageType",
    "disabled_language_message",
    "file_extensions_for_language",
    "file_globs_for_language",
    "is_query_language_enabled",
    "parse_query_language",
    "ripgrep_type_for_language",
]
