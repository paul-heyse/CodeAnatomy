"""Dataset scan/query helpers for Acero-backed plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from arrowdsl.compute.expr_core import ExprSpec
from arrowdsl.compute.filters import FilterSpec
from arrowdsl.compute.macros import FieldExpr
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, DeclarationLike, pc
from arrowdsl.core.schema_constants import PROVENANCE_COLS, PROVENANCE_SOURCE_FIELDS
from arrowdsl.plan.dataset_wrappers import DatasetLike
from arrowdsl.plan.scan_builder import ScanBuildSpec
from arrowdsl.plan.scan_telemetry import ScanTelemetry, fragment_telemetry
from arrowdsl.plan.source_normalize import (
    DatasetDiscoveryOptions,
    DatasetSourceOptions,
    PathLike,
    normalize_dataset_source,
)

type DatasetSourceLike = PathLike | DatasetLike
type ColumnsSpec = Sequence[str] | Mapping[str, ComputeExpression]

if TYPE_CHECKING:
    from arrowdsl.plan.plan import Plan


@dataclass(frozen=True)
class ProjectionSpec:
    """Defines the scan projection."""

    base: tuple[str, ...]
    derived: Mapping[str, ExprSpec] = field(default_factory=dict)


@dataclass(frozen=True)
class QuerySpec:
    """Declarative scan spec for a dataset."""

    projection: ProjectionSpec
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None

    def scan_columns(
        self,
        *,
        provenance: bool,
        scan_provenance: Sequence[str] = (),
        required_columns: Sequence[str] | None = None,
    ) -> ColumnsSpec:
        """Return the scan column spec for Arrow scanners.

        Returns
        -------
        ColumnsSpec
            Column spec for scanners or scan nodes.

        Raises
        ------
        ValueError
            Raised when a derived column is not scalar-safe.
        """
        if self.projection.derived:
            for name, expr in self.projection.derived.items():
                if not expr.is_scalar():
                    msg = f"QuerySpec.scan_columns: derived column {name!r} is not scalar-safe."
                    raise ValueError(msg)
        base_cols = list(self.projection.base)
        if required_columns is not None:
            required = set(required_columns)
            base_cols = [name for name in base_cols if name in required]
        if not provenance and not self.projection.derived and not scan_provenance:
            return list(base_cols)

        cols: dict[str, ComputeExpression] = {
            col: FieldExpr(col).to_expression() for col in base_cols
        }
        cols.update({name: expr.to_expression() for name, expr in self.projection.derived.items()})
        for name in scan_provenance:
            cols.setdefault(name, FieldExpr(name).to_expression())

        if provenance:
            cols.update(
                {
                    name: FieldExpr(PROVENANCE_SOURCE_FIELDS[name]).to_expression()
                    for name in PROVENANCE_COLS
                }
            )
        return cols

    def predicate_expression(self) -> ComputeExpression | None:
        """Return the plan-lane predicate expression, if any.

        Returns
        -------
        ComputeExpression | None
            Predicate expression or ``None`` when not defined.
        """
        filt = self.filter_spec()
        if filt is None:
            return None
        return filt.to_expression()

    def pushdown_expression(self) -> ComputeExpression | None:
        """Return the scan pushdown predicate expression, if any.

        Returns
        -------
        ComputeExpression | None
            Pushdown predicate expression or ``None`` when not defined.
        """
        filt = self.pushdown_filter_spec()
        if filt is None:
            return None
        return filt.to_expression()

    def filter_spec(self) -> FilterSpec | None:
        """Return the filter spec for plan-lane filtering, if any.

        Returns
        -------
        FilterSpec | None
            Filter specification or ``None`` when not defined.
        """
        if self.predicate is None:
            return None
        return FilterSpec(self.predicate)

    def pushdown_filter_spec(self) -> FilterSpec | None:
        """Return the filter spec for scan pushdown, if any.

        Returns
        -------
        FilterSpec | None
            Filter specification or ``None`` when not defined.
        """
        if self.pushdown_predicate is None:
            return None
        return FilterSpec(self.pushdown_predicate)

    def apply_to_plan(
        self,
        plan: Plan,
        *,
        ctx: ExecutionContext,
        scan_provenance: Sequence[str] | None = None,
        required_columns: Sequence[str] | None = None,
    ) -> Plan:
        """Apply filters and projections to a plan.

        Returns
        -------
        Plan
            Updated plan with filters/projections applied.
        """
        predicate = self.predicate_expression()
        if predicate is not None:
            plan = plan.filter(predicate, ctx=ctx)
        scan_provenance = (
            ctx.runtime.scan.scan_provenance_columns if scan_provenance is None else scan_provenance
        )
        cols = self.scan_columns(
            provenance=ctx.provenance,
            scan_provenance=scan_provenance,
            required_columns=required_columns,
        )
        available = set(plan.schema(ctx=ctx).names)
        if isinstance(cols, Mapping):
            derived_names = set(self.projection.derived)
            cols = {
                name: expr
                for name, expr in cols.items()
                if name in available or name in derived_names
            }
        else:
            cols = [name for name in cols if name in available]
        if isinstance(cols, Mapping):
            names = list(cols.keys())
            expressions = list(cols.values())
        else:
            names = list(cols)
            expressions = [pc.field(name) for name in cols]
        return plan.project(expressions, names, ctx=ctx)

    def to_plan(
        self,
        *,
        dataset: DatasetLike,
        ctx: ExecutionContext,
        label: str = "",
        scan_provenance: Sequence[str] = (),
    ) -> Plan:
        """Compile the query spec into an Acero-backed Plan.

        Returns
        -------
        Plan
            Plan representing the scan/filter/project pipeline.
        """
        builder = ScanBuildSpec(
            dataset=dataset,
            query=self,
            ctx=ctx,
            scan_provenance=tuple(scan_provenance),
        )
        return builder.to_plan(label=label)

    @staticmethod
    def simple(
        *cols: str,
        predicate: ExprSpec | None = None,
        pushdown_predicate: ExprSpec | None = None,
    ) -> QuerySpec:
        """Build a simple QuerySpec from column names.

        Returns
        -------
        QuerySpec
            Query specification instance.
        """
        return QuerySpec(
            projection=ProjectionSpec(base=tuple(cols)),
            predicate=predicate,
            pushdown_predicate=pushdown_predicate,
        )


def open_dataset(
    source: DatasetSourceLike,
    *,
    options: DatasetSourceOptions | None = None,
) -> DatasetLike:
    """Open a dataset for scanning.

    Returns
    -------
    DatasetLike
        Dataset instance for scanning.
    """
    return normalize_dataset_source(source, options=options)


def compile_to_acero_scan(
    dataset: DatasetLike,
    *,
    spec: QuerySpec,
    ctx: ExecutionContext,
    scan_provenance: Sequence[str] = (),
) -> DeclarationLike:
    """Compile a scan + filter + project pipeline into an Acero Declaration.

    Returns
    -------
    DeclarationLike
        Acero declaration for the scan pipeline.
    """
    builder = ScanBuildSpec(
        dataset=dataset,
        query=spec,
        ctx=ctx,
        scan_provenance=tuple(scan_provenance),
    )
    return builder.acero_decl()


__all__ = [
    "ColumnsSpec",
    "DatasetDiscoveryOptions",
    "DatasetSourceLike",
    "DatasetSourceOptions",
    "PathLike",
    "ProjectionSpec",
    "QuerySpec",
    "ScanTelemetry",
    "compile_to_acero_scan",
    "fragment_telemetry",
    "open_dataset",
]
