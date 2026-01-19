"""Scan builder utilities for ArrowDSL dataset plans."""

from __future__ import annotations

import importlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, cast

import pyarrow.dataset as ds

from arrowdsl.compile.plan_compiler import PlanCompiler
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, DeclarationLike, pc
from arrowdsl.core.schema_constants import PROVENANCE_COLS, PROVENANCE_SOURCE_FIELDS
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.builder import PlanBuilder
from arrowdsl.plan.dataset_wrappers import DatasetLike
from arrowdsl.plan.ops import scan_ordering_effect
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisQuerySpec

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan

type ColumnsSpec = Sequence[str] | Mapping[str, ComputeExpression]


def _ensure_expr_ir(expr: object, *, label: str) -> ExprIR:
    if isinstance(expr, ExprIR):
        return expr
    msg = f"ScanBuildSpec requires ExprIR for {label} expressions."
    raise TypeError(msg)


def _expr_ir_expression(expr: object | None, *, label: str) -> ComputeExpression | None:
    if expr is None:
        return None
    return _ensure_expr_ir(expr, label=label).to_expression()


def _expr_ir_is_scalar(expr: object, *, label: str) -> bool:
    return _ensure_expr_ir(expr, label=label).to_expr_spec().is_scalar()


@cache
def _plan_class() -> type[Plan]:
    module = importlib.import_module("arrowdsl.plan.plan")
    return cast("type[Plan]", module.Plan)


@dataclass(frozen=True)
class ScanBuildSpec:
    """Shared scan builder for scanners, Acero declarations, and plans."""

    dataset: DatasetLike
    query: IbisQuerySpec
    ctx: ExecutionContext
    scan_provenance: tuple[str, ...] | None = None
    required_columns: Sequence[str] | None = None

    def scan_columns(self) -> ColumnsSpec:
        """Return the scan column spec for this build context.

        Returns
        -------
        ColumnsSpec
            Scan column spec for scanners and scan nodes.

        Raises
        ------
        ValueError
            Raised when a derived column is not scalar-safe for scan projection.
        """
        derived = self.query.projection.derived
        if derived:
            for name, expr in derived.items():
                if not _expr_ir_is_scalar(expr, label=f"derived column {name!r}"):
                    msg = f"ScanBuildSpec.scan_columns: derived column {name!r} is not scalar-safe."
                    raise ValueError(msg)
        scan_provenance = (
            self.scan_provenance
            if self.scan_provenance is not None
            else self.ctx.runtime.scan.scan_provenance_columns
        )
        base_cols = list(self.query.projection.base)
        if self.required_columns is not None:
            required = set(self.required_columns)
            base_cols = [name for name in base_cols if name in required]
        if not self.ctx.provenance and not derived and not scan_provenance:
            return list(base_cols)
        cols: dict[str, ComputeExpression] = {col: pc.field(col) for col in base_cols}
        for name, expr in derived.items():
            cols[name] = _ensure_expr_ir(expr, label=f"derived column {name!r}").to_expression()
        for name in scan_provenance:
            cols.setdefault(name, pc.field(name))
        if self.ctx.provenance:
            cols.update(
                {name: pc.field(PROVENANCE_SOURCE_FIELDS[name]) for name in PROVENANCE_COLS}
            )
        return cols

    def predicate_expression(self) -> ComputeExpression | None:
        """Return the plan-lane predicate expression, if any.

        Returns
        -------
        ComputeExpression | None
            Predicate expression or ``None`` when not defined.
        """
        return _expr_ir_expression(self.query.predicate, label="predicate")

    def pushdown_expression(self) -> ComputeExpression | None:
        """Return the scan pushdown predicate expression, if any.

        Returns
        -------
        ComputeExpression | None
            Pushdown predicate expression or ``None`` when not defined.
        """
        return _expr_ir_expression(self.query.pushdown_predicate, label="pushdown predicate")

    def scanner(self) -> ds.Scanner:
        """Return a dataset scanner for the scan spec.

        Returns
        -------
        ds.Scanner
            Scanner configured with runtime scan options.
        """
        predicate = self.pushdown_expression()
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
        predicate = self.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=predicate,
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := self.predicate_expression()) is not None:
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
        predicate = self.pushdown_expression()
        _validate_predicate_fields(predicate, scan_provenance=self.scan_provenance)
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=predicate,
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := self.predicate_expression()) is not None:
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
