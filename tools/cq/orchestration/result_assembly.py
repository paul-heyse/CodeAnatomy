"""Result assembly helpers for multi-language orchestration."""

from __future__ import annotations

from collections.abc import Sequence

from tools.cq.core.schema import CqResult

__all__ = ["select_primary_result"]


def select_primary_result(results: Sequence[CqResult]) -> CqResult | None:
    """Select first non-empty result from ordered language results.

    Returns:
        CqResult | None: Primary result candidate, when available.
    """
    for result in results:
        if result.key_findings or result.sections or result.evidence:
            return result
    return results[0] if results else None
