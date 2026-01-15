"""Scan builder utilities for ArrowDSL dataset plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow.dataset as ds

from arrowdsl.compile.plan_compiler import PlanCompiler
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import DeclarationLike
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.builder import PlanBuilder
from arrowdsl.plan.ops import scan_ordering_effect
from arrowdsl.plan.plan import Plan

if TYPE_CHECKING:
    from arrowdsl.plan.query import ColumnsSpec, QuerySpec


@dataclass(frozen=True)
class ScanBuildSpec:
    """Shared scan builder for scanners, Acero declarations, and plans."""

    dataset: ds.Dataset
    query: QuerySpec
    ctx: ExecutionContext
    scan_provenance: tuple[str, ...] | None = None

    def scan_columns(self) -> ColumnsSpec:
        """Return the scan column spec for this build context.

        Returns
        -------
        ColumnsSpec
            Scan column spec for scanners and scan nodes.
        """
        scan_provenance = (
            self.scan_provenance
            if self.scan_provenance is not None
            else self.ctx.runtime.scan.scan_provenance_columns
        )
        return self.query.scan_columns(
            provenance=self.ctx.provenance,
            scan_provenance=scan_provenance,
        )

    def scanner(self) -> ds.Scanner:
        """Return a dataset scanner for the scan spec.

        Returns
        -------
        ds.Scanner
            Scanner configured with runtime scan options.
        """
        return self.dataset.scanner(
            columns=self.scan_columns(),
            filter=self.query.pushdown_expression(),
            **self.ctx.runtime.scan.scanner_kwargs(),
        )

    def acero_decl(self) -> DeclarationLike:
        """Compile the scan spec into an Acero declaration.

        Returns
        -------
        DeclarationLike
            Acero declaration for the scan pipeline.
        """
        builder = PlanBuilder()
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=self.query.pushdown_expression(),
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := self.query.predicate_expression()) is not None:
            builder.filter(predicate=predicate)
        plan_ir, _, _ = builder.build()
        return PlanCompiler(catalog=OP_CATALOG).to_acero(plan_ir, ctx=self.ctx)

    def to_plan(self, *, label: str = "") -> Plan:
        """Build a Plan from the scan spec.

        Returns
        -------
        Plan
            Plan representing the scan/filter pipeline.
        """
        builder = PlanBuilder()
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=self.query.pushdown_expression(),
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := self.query.predicate_expression()) is not None:
            builder.filter(predicate=predicate)
        plan_ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=plan_ir,
            label=label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )


__all__ = ["ScanBuildSpec"]
