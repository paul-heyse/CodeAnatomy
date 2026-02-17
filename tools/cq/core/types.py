"""Core shared enum/types for CQ modules."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal, cast

QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]
RipgrepLanguageType = Literal["py", "rust"]

DEFAULT_QUERY_LANGUAGE: QueryLanguage = "python"
DEFAULT_QUERY_LANGUAGE_SCOPE: QueryLanguageScope = "auto"
SUPPORTED_QUERY_LANGUAGES: tuple[QueryLanguage, ...] = ("python", "rust")
SUPPORTED_QUERY_LANGUAGE_SCOPES: tuple[QueryLanguageScope, ...] = ("auto", "python", "rust")
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


def _dedupe_globs(globs: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for glob in globs:
        if glob in seen:
            continue
        seen.add(glob)
        unique.append(glob)
    return unique


def expand_language_scope(scope: QueryLanguageScope) -> tuple[QueryLanguage, ...]:
    """Expand a scope token into ordered concrete languages.

    Returns:
        tuple[QueryLanguage, ...]: Ordered concrete languages in scope.
    """
    if scope == "auto":
        return ("python", "rust")
    return (scope,)


def primary_language(scope: QueryLanguageScope) -> QueryLanguage:
    """Return the primary language for scoring and tie-break defaults."""
    return expand_language_scope(scope)[0]


def file_extensions_for_language(lang: QueryLanguage) -> tuple[str, ...]:
    """Return source-file extensions for one concrete language."""
    return LANGUAGE_FILE_EXTENSIONS.get(lang, LANGUAGE_FILE_EXTENSIONS[DEFAULT_QUERY_LANGUAGE])


def file_extensions_for_scope(scope: QueryLanguageScope) -> tuple[str, ...]:
    """Return merged source-file extensions for a scope."""
    extensions: list[str] = []
    for lang in expand_language_scope(scope):
        for extension in file_extensions_for_language(lang):
            if extension not in extensions:
                extensions.append(extension)
    return tuple(extensions)


def file_globs_for_language(lang: QueryLanguage) -> list[str]:
    """Return summary globs for one concrete language."""
    return LANGUAGE_FILE_GLOBS.get(lang, LANGUAGE_FILE_GLOBS[DEFAULT_QUERY_LANGUAGE])


def file_globs_for_scope(scope: QueryLanguageScope) -> list[str]:
    """Return merged summary globs for a scope."""
    globs: list[str] = []
    for lang in expand_language_scope(scope):
        for glob in file_globs_for_language(lang):
            if glob not in globs:
                globs.append(glob)
    return globs


def ripgrep_type_for_language(lang: QueryLanguage) -> RipgrepLanguageType:
    """Return ripgrep type token for one concrete language."""
    return LANGUAGE_RIPGREP_TYPES.get(lang, LANGUAGE_RIPGREP_TYPES[DEFAULT_QUERY_LANGUAGE])


def ripgrep_types_for_scope(scope: QueryLanguageScope) -> tuple[RipgrepLanguageType, ...]:
    """Return ordered ripgrep type tokens for every language in scope."""
    return tuple(ripgrep_type_for_language(lang) for lang in expand_language_scope(scope))


def _has_glob_magic(value: str) -> bool:
    return any(ch in value for ch in ("*", "?", "[", "]"))


def _expand_directory_glob(base_glob: str, suffixes: tuple[str, ...]) -> list[str]:
    base = base_glob if base_glob.endswith("/") else f"{base_glob}/"
    return [f"{base}**/*{suffix}" for suffix in suffixes]


def _constrain_single_include_glob(glob: str, suffixes: tuple[str, ...]) -> list[str]:
    normalized = glob.strip()
    constrained: list[str] = []
    if normalized.startswith("!"):
        constrained = [normalized]
    elif not normalized:
        constrained = []
    elif not _has_glob_magic(normalized):
        suffix = Path(normalized).suffix.lower()
        if suffix:
            constrained = [normalized] if suffix in suffixes else []
        else:
            constrained = _expand_directory_glob(normalized, suffixes)
    elif normalized.endswith("/**"):
        constrained = _expand_directory_glob(normalized[:-3], suffixes)
    elif normalized.endswith("**"):
        constrained = _expand_directory_glob(normalized[:-2], suffixes)
    else:
        constrained = [normalized]
    return constrained


def constrain_include_globs_for_language(
    include_globs: list[str] | None,
    lang: QueryLanguage,
) -> list[str] | None:
    """Constrain include globs to language-compatible source patterns.

    Returns:
        list[str] | None: Filtered include globs, or ``None`` when none remain.
    """
    if not include_globs:
        return include_globs
    suffixes = LANGUAGE_FILE_EXTENSIONS[lang]
    constrained: list[str] = []
    for glob in include_globs:
        constrained.extend(_constrain_single_include_glob(glob, suffixes))
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
    """Infer concrete language from source-file extension.

    Returns:
        QueryLanguage | None: Inferred source language, if any.
    """
    suffix = Path(path).suffix.lower()
    for lang, extensions in LANGUAGE_FILE_EXTENSIONS.items():
        if suffix in extensions:
            return lang
    return None


def is_path_in_lang_scope(path: str | Path, scope: QueryLanguageScope) -> bool:
    """Return whether a path belongs to a language scope."""
    lang = infer_language_for_path(path)
    if lang is None:
        return False
    return lang in expand_language_scope(scope)


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
    "LANGUAGE_FILE_EXTENSIONS",
    "LANGUAGE_FILE_GLOBS",
    "LANGUAGE_RIPGREP_TYPES",
    "SUPPORTED_QUERY_LANGUAGES",
    "SUPPORTED_QUERY_LANGUAGE_SCOPES",
    "LdmdSliceMode",
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
    "is_python_language",
    "is_rust_language",
    "language_extension_exclude_globs",
    "parse_query_language",
    "parse_query_language_scope",
    "primary_language",
    "ripgrep_type_for_language",
    "ripgrep_types_for_scope",
]
