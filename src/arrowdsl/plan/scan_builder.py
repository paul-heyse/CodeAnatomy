"""Scan builder utilities for ArrowDSL dataset plans."""

from __future__ import annotations

import importlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, cast

import pyarrow.dataset as ds

from arrowdsl.compile.plan_compiler import PlanCompiler
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import DeclarationLike
from arrowdsl.core.schema_constants import PROVENANCE_COLS
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.builder import PlanBuilder
from arrowdsl.plan.ops import scan_ordering_effect

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan
    from arrowdsl.plan.query import ColumnsSpec, QuerySpec


@cache
def _plan_class() -> type[Plan]:
    module = importlib.import_module("arrowdsl.plan.plan")
    return cast("type[Plan]", module.Plan)


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
        predicate = self.query.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        return self.dataset.scanner(
            columns=self.scan_columns(),
            filter=predicate,
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
        predicate = self.query.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=predicate,
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
        predicate = self.query.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=predicate,
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := self.query.predicate_expression()) is not None:
            builder.filter(predicate=predicate)
        plan_ir, ordering, pipeline_breakers = builder.build()
        plan_cls = _plan_class()
        return plan_cls(
            ir=plan_ir,
            label=label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )


def _validate_predicate_fields(
    predicate: object | None,
    *,
    scan_provenance: Sequence[str] | None,
) -> None:
    if predicate is None:
        return
    predicate_text = str(predicate)
    forbidden = set(PROVENANCE_COLS)
    if scan_provenance:
        forbidden.update(scan_provenance)
    for name in sorted(forbidden):
        if re.search(rf"\\b{re.escape(name)}\\b", predicate_text):
            msg = f"Scan predicates must not reference provenance field {name!r}."
            raise ValueError(msg)


__all__ = ["ScanBuildSpec"]
