"""Enrichment phase orchestration for Smart Search partitions."""

from __future__ import annotations

from typing import cast

from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.contracts import SearchConfig, SearchPartitionPlanV1
from tools.cq.search.pipeline.partition_pipeline import run_search_partition
from tools.cq.search.pipeline.smart_search_types import LanguageSearchResult


def run_enrichment_phase(
    plan: SearchPartitionPlanV1,
    *,
    config: SearchConfig,
    mode: QueryMode,
) -> LanguageSearchResult:
    """Run partition cache/classification/enrichment pipeline for one language.

    Returns:
    -------
    LanguageSearchResult
        Partition result with matches, stats, and telemetry.
    """
    return cast("LanguageSearchResult", run_search_partition(plan, ctx=config, mode=mode))


__all__ = ["run_enrichment_phase"]
