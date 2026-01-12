"""Dataset scan/query helpers and pipeline runner."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.dataset as ds
import pyarrow.fs as pafs

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.compute.predicates import FilterSpec
from arrowdsl.core.context import ExecutionContext, OrderingEffect
from arrowdsl.core.interop import ComputeExpression, DeclarationLike, SchemaLike, TableLike, pc
from arrowdsl.finalize.finalize import Contract, FinalizeContext, FinalizeResult
from arrowdsl.plan.ops import FilterOp, KernelOp, ProjectOp, ScanOp
from arrowdsl.schema.arrays import FieldExpr
from core_types import StrictnessMode
from schema_spec import PROVENANCE_COLS, PROVENANCE_SOURCE_FIELDS, DatasetSpec

type PathLike = str | Path
type ColumnsSpec = Sequence[str] | Mapping[str, ComputeExpression]

KernelFn = Callable[[TableLike, ExecutionContext], TableLike]
KernelStep = KernelOp | KernelFn

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

    def scan_columns(self, *, provenance: bool) -> ColumnsSpec:
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
        if not provenance and not self.projection.derived:
            return list(self.projection.base)

        cols: dict[str, ComputeExpression] = {
            col: FieldExpr(col).to_expression() for col in self.projection.base
        }
        cols.update({name: expr.to_expression() for name, expr in self.projection.derived.items()})

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
    path: PathLike,
    *,
    dataset_format: str = "parquet",
    filesystem: pafs.FileSystem | None = None,
    partitioning: str | None = "hive",
    schema: SchemaLike | None = None,
) -> ds.Dataset:
    """Open a dataset for scanning.

    Returns
    -------
    ds.Dataset
        Dataset instance for scanning.
    """
    return ds.dataset(
        path,
        format=dataset_format,
        filesystem=filesystem,
        partitioning=partitioning,
        schema=schema,
    )


def make_scanner(dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext) -> ds.Scanner:
    """Create a dataset scanner under centralized scan policy.

    Returns
    -------
    ds.Scanner
        Scanner configured from the query spec.
    """
    return ds.Scanner.from_dataset(
        dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        filter=spec.pushdown_expression(),
        **ctx.runtime.scan.scanner_kwargs(),
    )


def scan_to_table(dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext) -> TableLike:
    """Materialize a dataset scan into a table.

    Returns
    -------
    TableLike
        Materialized table from the scan.
    """
    scanner = make_scanner(dataset, spec=spec, ctx=ctx)
    return scanner.to_table(use_threads=ctx.scan_use_threads)


def compile_to_acero_scan(
    dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext
) -> DeclarationLike:
    """Compile a scan + filter + project pipeline into an Acero Declaration.

    Returns
    -------
    DeclarationLike
        Acero declaration for the scan pipeline.
    """
    scan_ordering = (
        OrderingEffect.IMPLICIT
        if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
        else OrderingEffect.UNORDERED
    )
    scan_op = ScanOp(
        dataset=dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        predicate=spec.pushdown_expression(),
        ordering_effect=scan_ordering,
    )
    scan = scan_op.to_declaration([], ctx=ctx)

    filter_spec = spec.filter_spec()
    if filter_spec is not None:
        scan = FilterOp(predicate=filter_spec.to_expression()).to_declaration([scan], ctx=ctx)

    cols = spec.scan_columns(provenance=ctx.provenance)
    if isinstance(cols, dict):
        proj_exprs = list(cols.values())
        proj_names = list(cols.keys())
    else:
        proj_exprs = [pc.field(col) for col in cols]
        proj_names = list(cols)

    return ProjectOp(expressions=proj_exprs, names=proj_names).to_declaration([scan], ctx=ctx)


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


def run_pipeline(
    *,
    plan: Plan,
    spec: Contract | DatasetSpec,
    ctx: ExecutionContext,
    mode: StrictnessMode | None = None,
    post: Sequence[KernelStep] = (),
) -> FinalizeResult:
    """Execute a plan, apply kernels, and finalize results.

    Returns
    -------
    FinalizeResult
        Finalized table bundle.
    """
    if mode is not None:
        ctx = ctx.with_mode(mode)

    table = plan.to_table(ctx=ctx)
    for step in post:
        table = step.apply(table, ctx) if isinstance(step, KernelOp) else step(table, ctx)

    if isinstance(spec, DatasetSpec):
        return spec.finalize_context(ctx).run(table, ctx=ctx)

    transform = None
    if spec.schema_spec is not None:
        transform = spec.schema_spec.to_transform(
            safe_cast=ctx.safe_cast,
            on_error="unsafe" if ctx.safe_cast else "raise",
        )
    return FinalizeContext(contract=spec, transform=transform).run(table, ctx=ctx)


__all__ = [
    "ColumnsSpec",
    "KernelFn",
    "KernelStep",
    "PathLike",
    "ProjectionSpec",
    "QuerySpec",
    "ScanContext",
    "ScanTelemetry",
    "compile_to_acero_scan",
    "make_scanner",
    "open_dataset",
    "run_pipeline",
    "scan_to_table",
]
