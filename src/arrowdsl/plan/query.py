"""Dataset scan/query helpers for Acero-backed plans."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.fs as pafs

from arrowdsl.compute.expr import ExprSpec
from arrowdsl.compute.exprs import FieldExpr
from arrowdsl.compute.predicates import FilterSpec
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, DeclarationLike, SchemaLike, pc
from arrowdsl.plan.fragments import (
    fragment_file_hints,
    list_fragments,
    row_group_count,
    scan_task_count,
)
from arrowdsl.plan.ops import FilterOp, ProjectOp, ScanOp, scan_ordering_effect
from arrowdsl.plan.plan import Plan, PlanFactory
from arrowdsl.plan.scan_specs import DatasetFactorySpec, ScanSpec
from schema_spec import PROVENANCE_COLS, PROVENANCE_SOURCE_FIELDS

type PathLike = str | Path
type DatasetSourceLike = PathLike | ds.Dataset | DatasetFactorySpec
type ColumnsSpec = Sequence[str] | Mapping[str, ComputeExpression]


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

    def apply_to_plan(self, plan: Plan, *, ctx: ExecutionContext) -> Plan:
        """Apply filters and projections to a plan.

        Returns
        -------
        Plan
            Updated plan with filters/projections applied.
        """
        predicate = self.predicate_expression()
        if predicate is not None:
            plan = plan.filter(predicate, ctx=ctx)
        cols = self.scan_columns(provenance=ctx.provenance)
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
        dataset: ds.Dataset,
        ctx: ExecutionContext,
        label: str = "",
    ) -> Plan:
        """Compile the query spec into an Acero-backed Plan.

        Returns
        -------
        Plan
            Plan representing the scan/filter/project pipeline.
        """
        factory = PlanFactory(ctx=ctx)
        columns = self.scan_columns(provenance=ctx.provenance)
        plan = factory.scan(
            dataset,
            columns=columns,
            predicate=self.pushdown_expression(),
            label=label,
        )
        predicate = self.predicate_expression()
        if predicate is not None:
            plan = factory.filter(plan, predicate)
        if isinstance(columns, Mapping):
            names = list(columns.keys())
            expressions = list(columns.values())
        else:
            names = list(columns)
            expressions = [pc.field(name) for name in columns]
        return factory.project(plan, expressions=expressions, names=names)

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
    if isinstance(source, ds.Dataset):
        return source
    if isinstance(source, DatasetFactorySpec):
        return source.build(schema=schema)
    return ds.dataset(
        source,
        format=dataset_format,
        filesystem=filesystem,
        partitioning=partitioning,
        schema=schema,
    )


def compile_to_acero_scan(
    dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext
) -> DeclarationLike:
    """Compile a scan + filter + project pipeline into an Acero Declaration.

    Returns
    -------
    DeclarationLike
        Acero declaration for the scan pipeline.
    """
    scan_ordering = scan_ordering_effect(ctx)
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
    """Scan telemetry for dataset fragments."""

    fragment_count: int
    row_group_count: int
    estimated_rows: int | None
    file_hints: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScanContext:
    """Standardized dataset scan context."""

    dataset: ds.Dataset
    spec: QuerySpec
    ctx: ExecutionContext

    def telemetry(self) -> ScanTelemetry:
        """Return lightweight telemetry for the dataset scan.

        Returns
        -------
        ScanTelemetry
            Fragment counts and estimated row totals (when available).
        """
        predicate = self.spec.pushdown_expression()
        return fragment_telemetry(self.dataset, predicate=predicate, scanner=self.scanner())

    def scanner(self) -> ds.Scanner:
        """Return a dataset scanner for the scan spec.

        Returns
        -------
        ds.Scanner
            Scanner configured with runtime scan options.
        """
        columns = self.spec.scan_columns(provenance=self.ctx.provenance)
        predicate = self.spec.pushdown_expression()
        kwargs = self.ctx.runtime.scan.scanner_kwargs()
        return self.dataset.scanner(columns=columns, filter=predicate, **kwargs)

    def acero_decl(self) -> DeclarationLike:
        """Compile the scan to an Acero declaration.

        Returns
        -------
        DeclarationLike
            Acero declaration representing the scan.
        """
        return compile_to_acero_scan(self.dataset, spec=self.spec, ctx=self.ctx)

    def to_plan(self, *, label: str = "") -> Plan:
        """Compile the scan context into an Acero-backed Plan.

        Returns
        -------
        Plan
            Plan representing the scan/filter/project pipeline.
        """
        return self.spec.to_plan(dataset=self.dataset, ctx=self.ctx, label=label)


def scan_context_factory(
    spec: ScanSpec,
    *,
    ctx: ExecutionContext,
    schema: SchemaLike | None = None,
) -> ScanContext:
    """Return a ScanContext from a ScanSpec.

    Returns
    -------
    ScanContext
        Scan context configured for the spec and execution context.
    """
    dataset = spec.open_dataset(schema=schema)
    return ScanContext(dataset=dataset, spec=spec.query, ctx=ctx)


def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None = None,
    scanner: ds.Scanner | None = None,
    hint_limit: int | None = 5,
) -> ScanTelemetry:
    """Return telemetry for dataset fragments.

    Returns
    -------
    ScanTelemetry
        Fragment counts and estimated row totals (when available).
    """
    fragments = list_fragments(dataset, predicate=predicate)
    total_rows = 0
    estimated_rows: int | None = 0
    for fragment in fragments:
        metadata = fragment.metadata
        if metadata is None or metadata.num_rows is None or metadata.num_rows < 0:
            estimated_rows = None
            break
        total_rows += int(metadata.num_rows)
    if estimated_rows is not None:
        estimated_rows = total_rows
    if scanner is None:
        scanner = dataset.scanner(filter=predicate)
    try:
        task_count = scan_task_count(scanner)
    except (AttributeError, TypeError):
        task_count = row_group_count(fragments)
    return ScanTelemetry(
        fragment_count=len(fragments),
        row_group_count=task_count,
        estimated_rows=estimated_rows,
        file_hints=fragment_file_hints(fragments, limit=hint_limit),
    )


__all__ = [
    "ColumnsSpec",
    "DatasetSourceLike",
    "PathLike",
    "ProjectionSpec",
    "QuerySpec",
    "ScanContext",
    "ScanTelemetry",
    "compile_to_acero_scan",
    "fragment_telemetry",
    "open_dataset",
    "scan_context_factory",
]
