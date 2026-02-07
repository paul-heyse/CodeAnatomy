"""Parse pruning metrics from DataFusion explain-analyze output.

The DataFusion explain-analyze format varies across versions, so this
parser applies multiple regex patterns defensively and always returns
a valid ``PruningMetrics`` instance.
"""

from __future__ import annotations

import re

from datafusion_engine.pruning.metrics import PruningMetrics

# ---------------------------------------------------------------------------
# Regex patterns for various DataFusion explain-analyze formats
# ---------------------------------------------------------------------------

_ROW_GROUP_PRUNED_OF_RE = re.compile(
    r"pruned\s+(?P<pruned>\d+)\s+of\s+(?P<total>\d+)\s+row\s*groups",
    re.IGNORECASE,
)
_ROW_GROUP_PRUNED_EQ_RE = re.compile(
    r"pruned_groups\s*=\s*(?P<pruned>\d+)",
    re.IGNORECASE,
)
_ROW_GROUP_TOTAL_EQ_RE = re.compile(
    r"(?:total_groups|row_groups_total|row_groups)\s*=\s*(?P<total>\d+)",
    re.IGNORECASE,
)
_ROW_GROUP_STATS_PRUNED_RE = re.compile(
    r"RowGroupsStatisticsPruned\s*:\s*(?P<pruned>\d+)",
)

_PAGE_PRUNED_OF_RE = re.compile(
    r"pruned\s+(?P<pruned>\d+)\s+of\s+(?P<total>\d+)\s+pages",
    re.IGNORECASE,
)
_PAGE_PRUNED_EQ_RE = re.compile(
    r"pruned_pages\s*=\s*(?P<pruned>\d+)",
    re.IGNORECASE,
)
_PAGE_TOTAL_EQ_RE = re.compile(
    r"(?:total_pages|pages_total)\s*=\s*(?P<total>\d+)",
    re.IGNORECASE,
)

_FILTER_PUSHDOWN_RE = re.compile(
    r"(?:pushed_filters|predicate_pushdown|filter_pushdown)\s*=\s*(?P<count>\d+)",
    re.IGNORECASE,
)
_FILTER_EXPR_RE = re.compile(
    r"filters\s*=\s*\[(?P<filters>[^\]]*)\]",
    re.IGNORECASE,
)

_STATS_AVAILABLE_RE = re.compile(
    r"(?:statistics_available|has_statistics|stats)\s*=\s*(?:true|yes|1)",
    re.IGNORECASE,
)
_PARQUET_EXEC_RE = re.compile(r"ParquetExec", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_pruning_metrics(explain_text: str) -> PruningMetrics:
    """Parse pruning metrics from DataFusion explain-analyze output.

    Apply multiple regex patterns defensively to extract row-group,
    page, and filter-pushdown metrics.  Return zero-valued defaults
    when metrics cannot be extracted.

    Parameters
    ----------
    explain_text
        Raw text output from ``EXPLAIN ANALYZE``.

    Returns:
    -------
    PruningMetrics
        Parsed pruning metrics, with zeros for any missing data.
    """
    if not explain_text:
        return PruningMetrics()

    row_groups_total, row_groups_pruned = _parse_row_group_metrics(explain_text)
    pages_total, pages_pruned = _parse_page_metrics(explain_text)
    filters_pushed = _parse_filter_count(explain_text)
    statistics_available = _detect_statistics(explain_text)

    effectiveness = 0.0
    if row_groups_total > 0:
        effectiveness = row_groups_pruned / row_groups_total

    return PruningMetrics(
        row_groups_total=row_groups_total,
        row_groups_pruned=row_groups_pruned,
        pages_total=pages_total,
        pages_pruned=pages_pruned,
        filters_pushed=filters_pushed,
        statistics_available=statistics_available,
        pruning_effectiveness=effectiveness,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_row_group_metrics(text: str) -> tuple[int, int]:
    match = _ROW_GROUP_PRUNED_OF_RE.search(text)
    if match:
        return int(match.group("total")), int(match.group("pruned"))

    pruned = 0
    total = 0

    m_pruned = _ROW_GROUP_PRUNED_EQ_RE.search(text)
    if m_pruned:
        pruned = int(m_pruned.group("pruned"))

    m_stats_pruned = _ROW_GROUP_STATS_PRUNED_RE.search(text)
    if m_stats_pruned:
        pruned = max(pruned, int(m_stats_pruned.group("pruned")))

    m_total = _ROW_GROUP_TOTAL_EQ_RE.search(text)
    if m_total:
        total = int(m_total.group("total"))

    total = max(total, pruned)
    return total, pruned


def _parse_page_metrics(text: str) -> tuple[int, int]:
    match = _PAGE_PRUNED_OF_RE.search(text)
    if match:
        return int(match.group("total")), int(match.group("pruned"))

    pruned = 0
    total = 0

    m_pruned = _PAGE_PRUNED_EQ_RE.search(text)
    if m_pruned:
        pruned = int(m_pruned.group("pruned"))

    m_total = _PAGE_TOTAL_EQ_RE.search(text)
    if m_total:
        total = int(m_total.group("total"))

    total = max(total, pruned)
    return total, pruned


def _parse_filter_count(text: str) -> int:
    match = _FILTER_PUSHDOWN_RE.search(text)
    if match:
        return int(match.group("count"))

    match = _FILTER_EXPR_RE.search(text)
    if match:
        filter_content = match.group("filters").strip()
        if filter_content:
            return len([f for f in filter_content.split(",") if f.strip()])
    return 0


def _detect_statistics(text: str) -> bool:
    if _STATS_AVAILABLE_RE.search(text):
        return True
    return bool(
        _PARQUET_EXEC_RE.search(text)
        and (_ROW_GROUP_PRUNED_OF_RE.search(text) or _ROW_GROUP_PRUNED_EQ_RE.search(text))
    )


__all__ = [
    "parse_pruning_metrics",
]
