"""Canonical incremental enrichment mode vocabulary."""

from __future__ import annotations

import enum


class IncrementalEnrichmentModeV1(enum.StrEnum):
    """Canonical incremental enrichment mode for search integration."""

    TS_ONLY = "ts_only"
    TS_SYM = "ts_sym"
    TS_SYM_DIS = "ts_sym_dis"
    FULL = "full"

    @property
    def includes_symtable(self) -> bool:
        """Return whether this mode enables symtable enrichment."""
        return self in {
            IncrementalEnrichmentModeV1.TS_SYM,
            IncrementalEnrichmentModeV1.TS_SYM_DIS,
            IncrementalEnrichmentModeV1.FULL,
        }

    @property
    def includes_dis(self) -> bool:
        """Return whether this mode enables bytecode/dis enrichment."""
        return self in {
            IncrementalEnrichmentModeV1.TS_SYM_DIS,
            IncrementalEnrichmentModeV1.FULL,
        }

    @property
    def includes_inspect(self) -> bool:
        """Return whether this mode enables inspect/runtime enrichment."""
        return self is IncrementalEnrichmentModeV1.FULL


DEFAULT_INCREMENTAL_ENRICHMENT_MODE = IncrementalEnrichmentModeV1.TS_SYM


def parse_incremental_enrichment_mode(
    value: object,
    *,
    default: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE,
) -> IncrementalEnrichmentModeV1:
    """Parse user/config value into canonical incremental enrichment mode.

    Returns:
        IncrementalEnrichmentModeV1: Parsed mode or the provided default.
    """
    if isinstance(value, IncrementalEnrichmentModeV1):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        for mode in IncrementalEnrichmentModeV1:
            if mode.value == text:
                return mode
    return default


__all__ = [
    "DEFAULT_INCREMENTAL_ENRICHMENT_MODE",
    "IncrementalEnrichmentModeV1",
    "parse_incremental_enrichment_mode",
]
