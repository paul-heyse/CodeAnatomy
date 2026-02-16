"""Shared helpers for semantic search pipeline modules."""

from __future__ import annotations


def normalize_python_semantic_degradation_reason(
    *,
    reasons: tuple[str, ...],
    coverage_reason: str | None,
) -> str:
    """Normalize python semantic degradation reason across pipeline modules.

    Returns:
        str: Canonical degradation reason token for telemetry/reporting.
    """
    explicit_reasons = {
        "unsupported_capability",
        "capability_probe_unavailable",
        "request_interface_unavailable",
        "request_failed",
        "request_timeout",
        "session_unavailable",
        "timeout",
        "no_signal",
    }

    normalized_coverage_reason = coverage_reason
    if normalized_coverage_reason and normalized_coverage_reason.startswith(
        "no_python_semantic_signal:"
    ):
        normalized_coverage_reason = normalized_coverage_reason.removeprefix(
            "no_python_semantic_signal:"
        )
    if normalized_coverage_reason == "timeout":
        normalized_coverage_reason = "request_timeout"
    if (
        isinstance(normalized_coverage_reason, str)
        and normalized_coverage_reason in explicit_reasons
    ):
        return normalized_coverage_reason
    if coverage_reason:
        return "no_signal"

    for reason in reasons:
        normalized_reason = "request_timeout" if reason == "timeout" else reason
        if normalized_reason in explicit_reasons and normalized_reason != "no_signal":
            return normalized_reason
    return "no_signal"


def count_mapping_rows(value: object) -> int:
    """Count dictionary rows in list payloads.

    Returns:
        int: Number of dictionary rows when payload is a list.
    """
    if not isinstance(value, list):
        return 0
    return sum(1 for row in value if isinstance(row, dict))


__all__ = [
    "count_mapping_rows",
    "normalize_python_semantic_degradation_reason",
]
