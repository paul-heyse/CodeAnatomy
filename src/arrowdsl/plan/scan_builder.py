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
from arrowdsl.plan.dataset_wrappers import DatasetLike
from arrowdsl.plan.ops import scan_ordering_effect
from arrowdsl.plan.query_adapter import ibis_query_to_plan_query
from ibis_engine.query_compiler import IbisQuerySpec

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan
    from arrowdsl.plan.query import ColumnsSpec, QuerySpec


def _plan_query(query: QuerySpec | IbisQuerySpec) -> QuerySpec:
    if isinstance(query, IbisQuerySpec):
        return ibis_query_to_plan_query(query)
    return query


@cache
def _plan_class() -> type[Plan]:
    module = importlib.import_module("arrowdsl.plan.plan")
    return cast("type[Plan]", module.Plan)


@dataclass(frozen=True)
class ScanBuildSpec:
    """Shared scan builder for scanners, Acero declarations, and plans."""

    dataset: DatasetLike
    query: QuerySpec | IbisQuerySpec
    ctx: ExecutionContext
    scan_provenance: tuple[str, ...] | None = None
    required_columns: Sequence[str] | None = None

    def scan_columns(self) -> ColumnsSpec:
        """Return the scan column spec for this build context.

        Returns
        -------
        ColumnsSpec
            Scan column spec for scanners and scan nodes.
        """
        plan_query = _plan_query(self.query)
        scan_provenance = (
            self.scan_provenance
            if self.scan_provenance is not None
            else self.ctx.runtime.scan.scan_provenance_columns
        )
        return plan_query.scan_columns(
            provenance=self.ctx.provenance,
            scan_provenance=scan_provenance,
            required_columns=self.required_columns,
        )

    def scanner(self) -> ds.Scanner:
        """Return a dataset scanner for the scan spec.

        Returns
        -------
        ds.Scanner
            Scanner configured with runtime scan options.
        """
        plan_query = _plan_query(self.query)
        predicate = plan_query.pushdown_expression()
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
        plan_query = _plan_query(self.query)
        predicate = plan_query.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=predicate,
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := plan_query.predicate_expression()) is not None:
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
        plan_query = _plan_query(self.query)
        predicate = plan_query.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=predicate,
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := plan_query.predicate_expression()) is not None:
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
