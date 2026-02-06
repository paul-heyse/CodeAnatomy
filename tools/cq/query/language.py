"""Language and scope helpers for CQ query/search execution."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]
RipgrepLanguageType = Literal["py", "rust"]

# Concrete-language defaults for internals that operate on one language.
DEFAULT_QUERY_LANGUAGE: QueryLanguage = "python"
# Public execution default: unified multi-language scope.
DEFAULT_QUERY_LANGUAGE_SCOPE: QueryLanguageScope = "auto"

SUPPORTED_QUERY_LANGUAGES: tuple[QueryLanguage, ...] = ("python", "rust")
SUPPORTED_QUERY_LANGUAGE_SCOPES: tuple[QueryLanguageScope, ...] = (
    "auto",
    "python",
    "rust",
)

LANGUAGE_FILE_EXTENSIONS: dict[QueryLanguage, tuple[str, ...]] = {
    "python": (".py", ".pyi"),
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


def _dedupe_globs(globs: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for glob in globs:
        if glob in seen:
            continue
        seen.add(glob)
        unique.append(glob)
    return unique


def parse_query_language(value: str) -> QueryLanguage:
    """Parse a concrete language token.

    Args:
        value: Candidate language token.

    Returns:
        QueryLanguage: Normalized concrete query language.

    Raises:
        ValueError: If the language token is not supported.
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
    """Parse a language scope token, defaulting to ``auto`` when omitted.

    Args:
        value: Candidate language scope token, or `None`.

    Returns:
        QueryLanguageScope: Normalized language scope.

    Raises:
        ValueError: If the scope token is not supported.
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


def expand_language_scope(scope: QueryLanguageScope) -> tuple[QueryLanguage, ...]:
    """Expand a scope into ordered concrete languages.

    Returns:
        tuple[QueryLanguage, ...]: Ordered concrete languages for the given scope.
    """
    if scope == "auto":
        return ("python", "rust")
    return (scope,)


def primary_language(scope: QueryLanguageScope) -> QueryLanguage:
    """Return the primary language for scoring/tie-break defaults.

    Returns:
        QueryLanguage: Primary language for the scope.
    """
    return expand_language_scope(scope)[0]


def file_extensions_for_language(lang: QueryLanguage) -> tuple[str, ...]:
    """Return extensions for a concrete language.

    Returns:
    -------
    tuple[str, ...]
        Supported source extensions.
    """
    return LANGUAGE_FILE_EXTENSIONS.get(lang, LANGUAGE_FILE_EXTENSIONS[DEFAULT_QUERY_LANGUAGE])


def file_extensions_for_scope(scope: QueryLanguageScope) -> tuple[str, ...]:
    """Return merged extensions for a language scope.

    Returns:
    -------
    tuple[str, ...]
        De-duplicated extensions across all scoped languages.
    """
    extensions: list[str] = []
    for lang in expand_language_scope(scope):
        for extension in file_extensions_for_language(lang):
            if extension not in extensions:
                extensions.append(extension)
    return tuple(extensions)


def file_globs_for_language(lang: QueryLanguage) -> list[str]:
    """Return summary globs for a concrete language.

    Returns:
    -------
    list[str]
        Summary glob patterns for the language.
    """
    return LANGUAGE_FILE_GLOBS.get(lang, LANGUAGE_FILE_GLOBS[DEFAULT_QUERY_LANGUAGE])


def file_globs_for_scope(scope: QueryLanguageScope) -> list[str]:
    """Return merged summary globs for a language scope.

    Returns:
    -------
    list[str]
        De-duplicated summary globs across all scoped languages.
    """
    globs: list[str] = []
    for lang in expand_language_scope(scope):
        for glob in file_globs_for_language(lang):
            if glob not in globs:
                globs.append(glob)
    return globs


def ripgrep_type_for_language(lang: QueryLanguage) -> RipgrepLanguageType:
    """Return ripgrep type token for a concrete language.

    Returns:
    -------
    RipgrepLanguageType
        Ripgrep type token.
    """
    return LANGUAGE_RIPGREP_TYPES.get(lang, LANGUAGE_RIPGREP_TYPES[DEFAULT_QUERY_LANGUAGE])


def ripgrep_types_for_scope(scope: QueryLanguageScope) -> tuple[RipgrepLanguageType, ...]:
    """Return ordered ripgrep type tokens for a language scope.

    Returns:
    -------
    tuple[RipgrepLanguageType, ...]
        Ordered ripgrep type tokens for all languages in scope.
    """
    return tuple(ripgrep_type_for_language(lang) for lang in expand_language_scope(scope))


def constrain_include_globs_for_language(
    include_globs: list[str] | None,
    lang: QueryLanguage,
) -> list[str] | None:
    """Constrain include globs to language-compatible file patterns."""
    if not include_globs:
        return include_globs
    suffixes = LANGUAGE_FILE_EXTENSIONS[lang]
    constrained: list[str] = []
    for glob in include_globs:
        normalized = glob.strip()
        if not normalized:
            continue
        if normalized.startswith("!"):
            constrained.append(normalized)
            continue
        if normalized.endswith("/**"):
            base = normalized[:-3]
            constrained.extend(f"{base}**/*{suffix}" for suffix in suffixes)
            continue
        if normalized.endswith("**"):
            base = normalized[:-2]
            constrained.extend(f"{base}**/*{suffix}" for suffix in suffixes)
            continue
        constrained.append(normalized)
    return _dedupe_globs(constrained) if constrained else None


def language_extension_exclude_globs(lang: QueryLanguage) -> tuple[str, ...]:
    """Return extension-level exclusion globs for non-target CQ languages."""
    excludes: list[str] = []
    for candidate, extensions in LANGUAGE_FILE_EXTENSIONS.items():
        if candidate == lang:
            continue
        excludes.extend(f"*{extension}" for extension in extensions)
    return tuple(excludes)


def infer_language_for_path(path: str | Path) -> QueryLanguage | None:
    """Infer concrete language from file extension.

    Args:
        path: File path string or path object.

    Returns:
        QueryLanguage | None: Inferred language, or ``None`` for non-source files.
    """
    suffix = Path(path).suffix.lower()
    for lang, extensions in LANGUAGE_FILE_EXTENSIONS.items():
        if suffix in extensions:
            return lang
    return None


def is_path_in_lang_scope(path: str | Path, scope: QueryLanguageScope) -> bool:
    """Check whether a path belongs to a language scope.

    Args:
        path: File path to validate.
        scope: Target language scope.

    Returns:
        bool: ``True`` when the file extension belongs to the scope.
    """
    lang = infer_language_for_path(path)
    if lang is None:
        return False
    return lang in expand_language_scope(scope)


__all__ = [
    "DEFAULT_QUERY_LANGUAGE",
    "DEFAULT_QUERY_LANGUAGE_SCOPE",
    "LANGUAGE_FILE_EXTENSIONS",
    "LANGUAGE_FILE_GLOBS",
    "LANGUAGE_RIPGREP_TYPES",
    "SUPPORTED_QUERY_LANGUAGES",
    "SUPPORTED_QUERY_LANGUAGE_SCOPES",
    "QueryLanguage",
    "QueryLanguageScope",
    "RipgrepLanguageType",
    "constrain_include_globs_for_language",
    "expand_language_scope",
    "file_extensions_for_language",
    "file_extensions_for_scope",
    "file_globs_for_language",
    "file_globs_for_scope",
    "infer_language_for_path",
    "is_path_in_lang_scope",
    "language_extension_exclude_globs",
    "parse_query_language",
    "parse_query_language_scope",
    "primary_language",
    "ripgrep_type_for_language",
    "ripgrep_types_for_scope",
]
