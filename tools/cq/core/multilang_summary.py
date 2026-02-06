"""Shared multi-language summary contract helpers for CQ results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from tools.cq.query.language import QueryLanguage, QueryLanguageScope, expand_language_scope
from tools.cq.search.contracts import (
    CrossLanguageDiagnostic,
    LanguageCapabilities,
    LanguagePartitionStats,
    SearchSummaryContract,
    coerce_diagnostics,
    coerce_language_capabilities,
    coerce_language_partitions,
    summary_contract_to_dict,
)

if TYPE_CHECKING:
    from tools.cq.search.contracts import EnrichmentTelemetry

_REQUIRED_KEYS: tuple[str, ...] = (
    "lang_scope",
    "language_order",
    "languages",
    "cross_language_diagnostics",
    "language_capabilities",
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
    normalized = coerce_language_partitions(
        language_order=ordered_languages,
        partitions=languages,
    )
    from tools.cq.core.serialization import to_builtins

    return {lang: dict(to_builtins(partition)) for lang, partition in normalized.items()}


def build_multilang_summary(  # noqa: PLR0913
    *,
    common: Mapping[str, object] | None,
    lang_scope: QueryLanguageScope,
    language_order: Sequence[QueryLanguage] | None,
    languages: Mapping[QueryLanguage, Mapping[str, object]],
    cross_language_diagnostics: Sequence[CrossLanguageDiagnostic | Mapping[str, object]]
    | None = None,
    language_capabilities: LanguageCapabilities | Mapping[str, object] | None = None,
    enrichment_telemetry: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build canonical multilang summary payload.

    Returns:
    -------
    dict[str, object]
        Summary dictionary with required multilang keys.
    """
    ordered_languages = (
        tuple(language_order) if language_order is not None else expand_language_scope(lang_scope)
    )
    contract = SearchSummaryContract(
        lang_scope=lang_scope,
        language_order=list(ordered_languages),
        languages=coerce_language_partitions(
            language_order=ordered_languages,
            partitions=languages,
        ),
        cross_language_diagnostics=coerce_diagnostics(cross_language_diagnostics),
        language_capabilities=coerce_language_capabilities(language_capabilities),
        enrichment_telemetry=(
            None
            if enrichment_telemetry is None
            else _coerce_enrichment_telemetry(enrichment_telemetry)
        ),
    )
    summary = summary_contract_to_dict(contract, common=common)
    assert_multilang_summary(summary)
    return summary


def _coerce_enrichment_telemetry(
    payload: Mapping[str, object],
) -> EnrichmentTelemetry | None:
    from tools.cq.search.contracts import EnrichmentTelemetry

    return EnrichmentTelemetry.from_mapping(payload)


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

    payload = LanguagePartitionStats(
        matches=matches,
        files_scanned=files_scanned,
        scanned_files=_coerce_int(summary.get("scanned_files"), files_scanned),
        scanned_files_is_estimate=_coerce_bool(
            summary.get("scanned_files_is_estimate"),
            default=False,
        ),
        matched_files=_coerce_int(summary.get("matched_files"), 0),
        total_matches=_coerce_int(summary.get("total_matches"), matches),
        timed_out=_coerce_bool(summary.get("timed_out"), default=False),
        truncated=_coerce_bool(summary.get("truncated"), default=False),
        caps_hit=str(summary.get("caps_hit", "none")),
        error=(
            str(summary.get("error"))
            if isinstance(summary.get("error"), str) and str(summary.get("error"))
            else None
        ),
    )
    from tools.cq.core.serialization import to_builtins

    return dict(to_builtins(payload))


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
    if not isinstance(diagnostics, list) or not all(isinstance(item, dict) for item in diagnostics):
        msg = "summary.cross_language_diagnostics must be a list[dict[str, object]]"
        raise TypeError(msg)

    language_capabilities = summary.get("language_capabilities")
    if not isinstance(language_capabilities, dict):
        msg = "summary.language_capabilities must be a dict[str, object]"
        raise TypeError(msg)


__all__ = [
    "LanguagePartitionStats",
    "assert_multilang_summary",
    "build_multilang_summary",
    "normalize_language_partitions",
    "partition_stats_from_result_summary",
]
