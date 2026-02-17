"""Shared utility functions for query subsystem."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.astgrep.sgpy_scanner import SgRecord
    from tools.cq.core.schema import CqResult
    from tools.cq.core.summary_contract import SummaryEnvelopeV1


def count_result_matches(result: CqResult | None) -> int:
    """Count total matches across all findings in result.

    Parameters
    ----------
    result : CqResult | None
        Query result to count matches from.

    Returns:
    -------
    int
        Total number of matches found in the result.
    """
    if result is None:
        return 0
    summary_matches = result.summary.matches
    if isinstance(summary_matches, int):
        return summary_matches
    summary_total = result.summary.total_matches
    if isinstance(summary_total, int):
        return summary_total
    return len(result.key_findings)


def extract_missing_languages(summary: SummaryEnvelopeV1) -> list[str]:
    """Extract missing_languages list from typed summary.

    Parameters
    ----------
    summary : SummaryEnvelopeV1
        Result summary containing language partition payload.

    Returns:
    -------
    list[str]
        List of language names that had no matches.
    """
    languages = summary.languages
    if not isinstance(languages, Mapping):
        return []
    missing: list[str] = []
    for lang, payload in languages.items():
        lang_name = str(lang)
        if not isinstance(payload, Mapping):
            missing.append(lang_name)
            continue
        matches = payload.get("matches")
        if isinstance(matches, int) and matches > 0:
            continue
        total = payload.get("total_matches")
        if isinstance(total, int):
            if total <= 0:
                missing.append(lang_name)
            continue
        if isinstance(matches, int) and matches <= 0:
            missing.append(lang_name)
    return missing


def extract_def_name(record: SgRecord) -> str | None:
    """Extract name from function/class definition record.

    Supports Python (def/class) and Rust (fn/struct/enum/trait) patterns.

    Parameters
    ----------
    record : SgRecord
        ast-grep record to extract name from.

    Returns:
    -------
    str | None
        Definition name when available, None otherwise.
    """
    text = record.text.lstrip()

    if record.record == "def":
        patterns = (
            r"(?:async\s+)?(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"fn\s+([A-Za-z_][A-Za-z0-9_]*)",
            r"(?:struct|enum|trait)\s+([A-Za-z_][A-Za-z0-9_]*)",
        )
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)

    return None


__all__ = ["count_result_matches", "extract_def_name", "extract_missing_languages"]
