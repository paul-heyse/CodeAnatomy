"""Pipeline orchestration: assembly bridge, section builders, and pipeline facade."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar, cast

from tools.cq.core.schema import CqResult, Finding, Section
from tools.cq.search.pipeline.contracts import SmartSearchContext

if TYPE_CHECKING:
    from tools.cq.search.pipeline.smart_search_types import SearchResultAssembly

TPartition = TypeVar("TPartition")

# --- From assembly.py ---


def assemble_result(assembly: SearchResultAssembly) -> CqResult:
    """Assemble final CQ result from precomputed partition output.

    Returns:
        CqResult: Function return value.
    """
    from tools.cq.search.pipeline.smart_search import assemble_smart_search_result

    pipeline = SearchPipeline(assembly.context)
    assembler = cast(
        "Callable[[SmartSearchContext, list[object]], CqResult]",
        assemble_smart_search_result,
    )
    return pipeline.assemble(assembly.partition_results, assembler)


# --- From section_builder.py ---


def insert_target_candidates(
    sections: list[Section],
    *,
    candidates: list[Finding],
) -> None:
    """Insert Target Candidates section at top when candidates exist."""
    if not candidates:
        return
    sections.insert(0, Section(title="Target Candidates", findings=candidates))


def insert_neighborhood_preview(
    sections: list[Section],
    *,
    findings: list[Finding],
    has_target_candidates: bool,
) -> None:
    """Insert Neighborhood Preview section after candidates when present."""
    if not findings:
        return
    insert_idx = 1 if has_target_candidates else 0
    sections.insert(insert_idx, Section(title="Neighborhood Preview", findings=findings))


# --- From legacy.py ---


@dataclass(slots=True)
class SearchPipeline:
    """Pipeline facade for search orchestration steps."""

    context: SmartSearchContext

    def run_partitions(
        self,
        partition_runner: Callable[[SmartSearchContext], TPartition],
    ) -> TPartition:
        """Run discovery/classification/enrichment partition stage.

        Returns:
            Opaque partition output from the runner callback.
        """
        return partition_runner(self.context)

    def assemble(
        self,
        partition_results: TPartition,
        assembler: Callable[[SmartSearchContext, TPartition], CqResult],
    ) -> CqResult:
        """Run final section/summary assembly stage.

        Returns:
            Final assembled CQ result.
        """
        return assembler(self.context, partition_results)

    def execute(
        self,
        partition_runner: Callable[[SmartSearchContext], TPartition],
        assembler: Callable[[SmartSearchContext, TPartition], CqResult],
    ) -> CqResult:
        """Execute candidate partitioning and result assembly.

        Returns:
            Assembled CQ result for the search context.
        """
        partition_results = self.run_partitions(partition_runner)
        return self.assemble(partition_results, assembler)


__all__ = [
    "SearchPipeline",
    "assemble_result",
    "insert_neighborhood_preview",
    "insert_target_candidates",
]
