"""Scan context utilities for dataset scans and Acero compilation."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow.dataset as ds

from arrowdsl.dataset_io import compile_to_acero_scan, make_scanner
from arrowdsl.pyarrow_protocols import DeclarationLike
from arrowdsl.queryspec import QuerySpec
from arrowdsl.runtime import ExecutionContext


@dataclass(frozen=True)
class ScanTelemetry:
    """Basic scan telemetry captured from Dataset primitives."""

    fragment_count: int
    estimated_rows: int | None


@dataclass(frozen=True)
class ScanContext:
    """Standardized dataset scan context."""

    dataset: ds.Dataset
    spec: QuerySpec
    ctx: ExecutionContext

    def telemetry(self) -> ScanTelemetry:
        """Collect fragment and row-count telemetry.

        Returns
        -------
        ScanTelemetry
            Fragment count and estimated rows.
        """
        predicate = self.spec.pushdown_expression()
        fragments = list(self.dataset.get_fragments(filter=predicate))
        try:
            rows = int(self.dataset.count_rows(filter=predicate))
        except (TypeError, ValueError):
            rows = None
        return ScanTelemetry(fragment_count=len(fragments), estimated_rows=rows)

    def scanner(self) -> ds.Scanner:
        """Build a Scanner using centralized scan policy.

        Returns
        -------
        ds.Scanner
            Dataset scanner configured from the query spec.
        """
        return make_scanner(self.dataset, spec=self.spec, ctx=self.ctx)

    def acero_decl(self) -> DeclarationLike:
        """Compile the scan to an Acero declaration.

        Returns
        -------
        DeclarationLike
            Acero declaration representing the scan.
        """
        return compile_to_acero_scan(self.dataset, spec=self.spec, ctx=self.ctx)
