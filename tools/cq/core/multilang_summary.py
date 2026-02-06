"""Shared multi-language summary contract helpers for CQ results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypedDict

from tools.cq.query.language import (
    QueryLanguage,
    QueryLanguageScope,
    expand_language_scope,
)


class LanguagePartitionStats(TypedDict, total=False):
    """Per-language summary partition payload."""

    matches: int
    files_scanned: int
    scanned_files: int
    scanned_files_is_estimate: bool
    matched_files: int
    total_matches: int
    truncated: bool
    timed_out: bool
    caps_hit: str
    error: str


_REQUIRED_KEYS: tuple[str, ...] = (
    "lang_scope",
    "language_order",
    "languages",
    "cross_language_diagnostics",
)


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _coerce_bool(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    return default


def normalize_language_partitions(
    *,
    scope: QueryLanguageScope,
    language_order: Sequence[QueryLanguage] | None,
    languages: Mapping[QueryLanguage, Mapping[str, object]],
) -> dict[QueryLanguage, dict[str, object]]:
    """Normalize partition payloads in deterministic language order.

    Returns:
    -------
    dict[QueryLanguage, dict[str, object]]
        Ordered per-language summary payloads.
    """
    ordered_languages = (
        tuple(language_order) if language_order is not None else expand_language_scope(scope)
    )
    normalized: dict[QueryLanguage, dict[str, object]] = {}
    for lang in ordered_languages:
        payload = languages.get(lang)
        normalized[lang] = dict(payload) if payload is not None else {}
    # Preserve additional languages not in ordered scope (defensive).
    for lang, payload in languages.items():
        if lang not in normalized:
            normalized[lang] = dict(payload)
    return normalized


def build_multilang_summary(
    *,
    common: Mapping[str, object] | None,
    lang_scope: QueryLanguageScope,
    language_order: Sequence[QueryLanguage] | None,
    languages: Mapping[QueryLanguage, Mapping[str, object]],
    cross_language_diagnostics: Sequence[str] | None = None,
) -> dict[str, object]:
    """Build canonical multilang summary payload.

    Returns:
    -------
    dict[str, object]
        Summary dictionary with required multilang keys.
    """
    summary: dict[str, object] = dict(common) if common is not None else {}
    ordered_languages = (
        tuple(language_order) if language_order is not None else expand_language_scope(lang_scope)
    )
    summary["lang_scope"] = lang_scope
    summary["language_order"] = list(ordered_languages)
    summary["languages"] = normalize_language_partitions(
        scope=lang_scope,
        language_order=ordered_languages,
        languages=languages,
    )
    summary["cross_language_diagnostics"] = list(cross_language_diagnostics or [])
    assert_multilang_summary(summary)
    return summary


def partition_stats_from_result_summary(
    summary: Mapping[str, object],
    *,
    fallback_matches: int = 0,
) -> dict[str, object]:
    """Extract a compact language partition payload from a command summary.

    Returns:
    -------
    dict[str, object]
        Stable per-language stats map.
    """
    files_scanned = _coerce_int(summary.get("files_scanned"), 0)
    if files_scanned == 0:
        files_scanned = _coerce_int(summary.get("scanned_files"), 0)
    matches = _coerce_int(summary.get("matches"), fallback_matches)
    if matches == 0:
        matches = _coerce_int(summary.get("total_matches"), fallback_matches)

    payload: dict[str, object] = {
        "matches": matches,
        "files_scanned": files_scanned,
        "scanned_files": _coerce_int(summary.get("scanned_files"), files_scanned),
        "scanned_files_is_estimate": _coerce_bool(
            summary.get("scanned_files_is_estimate"),
            default=False,
        ),
        "matched_files": _coerce_int(summary.get("matched_files"), 0),
        "total_matches": _coerce_int(summary.get("total_matches"), matches),
        "timed_out": _coerce_bool(summary.get("timed_out"), default=False),
        "truncated": _coerce_bool(summary.get("truncated"), default=False),
        "caps_hit": str(summary.get("caps_hit", "none")),
    }
    error = summary.get("error")
    if isinstance(error, str) and error:
        payload["error"] = error
    return payload


def assert_multilang_summary(summary: Mapping[str, object]) -> None:
    """Validate required multi-language summary fields.

    Args:
        summary: Description.

    Raises:
        TypeError: If the operation cannot be completed.
        ValueError: If the operation cannot be completed.
    """
    missing = [key for key in _REQUIRED_KEYS if key not in summary]
    if missing:
        msg = f"Missing required multilang summary fields: {', '.join(missing)}"
        raise ValueError(msg)

    lang_scope = summary.get("lang_scope")
    if lang_scope not in {"auto", "python", "rust"}:
        msg = f"Invalid lang_scope in summary: {lang_scope!r}"
        raise ValueError(msg)

    language_order = summary.get("language_order")
    if not isinstance(language_order, list) or not all(
        isinstance(item, str) for item in language_order
    ):
        msg = "summary.language_order must be a list[str]"
        raise ValueError(msg)

    languages = summary.get("languages")
    if not isinstance(languages, dict):
        msg = "summary.languages must be a dict[str, dict[str, object]]"
        raise TypeError(msg)
    for key, value in languages.items():
        if not isinstance(key, str):
            msg = "summary.languages keys must be str"
            raise TypeError(msg)
        if not isinstance(value, dict):
            msg = f"summary.languages[{key!r}] must be dict[str, object]"
            raise TypeError(msg)

    diagnostics = summary.get("cross_language_diagnostics")
    if not isinstance(diagnostics, list) or not all(isinstance(item, str) for item in diagnostics):
        msg = "summary.cross_language_diagnostics must be a list[str]"
        raise ValueError(msg)


__all__ = [
    "LanguagePartitionStats",
    "assert_multilang_summary",
    "build_multilang_summary",
    "normalize_language_partitions",
    "partition_stats_from_result_summary",
]
