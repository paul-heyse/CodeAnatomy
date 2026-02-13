"""Smart-search pipeline orchestration facade."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from tools.cq.core.schema import CqResult
from tools.cq.search.context import SmartSearchContext


@dataclass(slots=True)
class SearchPipeline:
    """Pipeline facade for search orchestration steps."""

    context: SmartSearchContext

    def run_partitions(
        self,
        partition_runner: Callable[[SmartSearchContext], object],
    ) -> object:
        """Run discovery/classification/enrichment partition stage.

        Returns:
            Opaque partition output from the runner callback.
        """
        return partition_runner(self.context)

    def assemble(
        self,
        partition_results: object,
        assembler: Callable[[SmartSearchContext, object], CqResult],
    ) -> CqResult:
        """Run final section/summary assembly stage.

        Returns:
            Final assembled CQ result.
        """
        return assembler(self.context, partition_results)

    def execute(
        self,
        partition_runner: Callable[[SmartSearchContext], object],
        assembler: Callable[[SmartSearchContext, object], CqResult],
    ) -> CqResult:
        """Execute candidate partitioning and result assembly.

        Returns:
            Assembled CQ result for the search context.
        """
        partition_results = self.run_partitions(partition_runner)
        return self.assemble(partition_results, assembler)


__all__ = ["SearchPipeline"]
