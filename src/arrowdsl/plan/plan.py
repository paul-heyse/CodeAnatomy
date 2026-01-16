"""Plan wrapper around PlanIR for ArrowDSL fallback execution."""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa

from arrowdsl.core.context import (
    ExecutionContext,
    Ordering,
    OrderingKey,
    OrderingLevel,
)
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
    pc,
)
from arrowdsl.exec.runtime import run_segments
from arrowdsl.ir.plan import PlanIR
from arrowdsl.ops.catalog import OP_CATALOG
from arrowdsl.plan.builder import PlanBuilder
from arrowdsl.plan.ops import DedupeSpec, JoinSpec
from arrowdsl.plan.ordering_policy import apply_canonical_sort, ordering_metadata_for_plan
from arrowdsl.plan.planner import SegmentPlan, segment_plan
from arrowdsl.schema.metadata import merge_metadata_specs
from arrowdsl.schema.schema import SchemaMetadataSpec

EXPLODE_OUT_COLS_LEN = 2
_PLAN_SNAPSHOT_ENV = "ARROWDSL_PLAN_SNAPSHOT_PATH"
_PLAN_SCHEMA_LOG_ENV = "ARROWDSL_PLAN_SCHEMA_LOG_PATH"
_PLAN_SNAPSHOT_NODE_LIMIT = 200
_PLAN_SNAPSHOT_OP_LIMIT = 200

logger = logging.getLogger(__name__)


type PlanKind = Literal["reader", "table"]


@dataclass(frozen=True)
class PlanRunResult:
    """Plan output bundled with materialization metadata."""

    value: TableLike | RecordBatchReaderLike
    kind: PlanKind


@dataclass(frozen=True)
class PlanSpec:
    """Plan wrapper with pipeline-breaker metadata."""

    plan: Plan
    pipeline_breakers: tuple[str, ...] = ()

    @classmethod
    def from_plan(cls, plan: Plan) -> PlanSpec:
        """Create a PlanSpec from a Plan instance.

        Returns
        -------
        PlanSpec
            PlanSpec wrapper with pipeline-breaker metadata.
        """
        return cls(plan=plan, pipeline_breakers=plan.pipeline_breakers)

    def to_reader(self, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
        """Return a streaming reader if no pipeline breakers are present.

        Returns
        -------
        pyarrow.RecordBatchReader
            Streaming reader for the plan.

        Raises
        ------
        ValueError
            Raised when pipeline breakers are present.
        """
        if self.pipeline_breakers:
            msg = f"Plan contains pipeline breakers: {self.pipeline_breakers}"
            raise ValueError(msg)
        return self.plan.to_reader(ctx=ctx)

    def to_table(self, *, ctx: ExecutionContext) -> TableLike:
        """Materialize the plan as a table.

        Returns
        -------
        pyarrow.Table
            Materialized table.
        """
        return self.plan.to_table(ctx=ctx)

    def schema(self, *, ctx: ExecutionContext) -> SchemaLike:
        """Return the output schema for the plan.

        Returns
        -------
        pyarrow.Schema
            Output schema for the plan.
        """
        return self.plan.schema(ctx=ctx)


@dataclass(frozen=True)
class Plan:
    """Plan wrapper around PlanIR for fallback execution."""

    ir: PlanIR
    label: str = ""
    ordering: Ordering = field(default_factory=Ordering.unordered)
    pipeline_breakers: tuple[str, ...] = ()

    def to_reader(self, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
        """Return a streaming reader for this plan.

        Parameters
        ----------
        ctx:
            Execution context controlling threading.

        Returns
        -------
        pyarrow.RecordBatchReader
            Streaming reader.

        Raises
        ------
        ValueError
            Raised when the plan is not streamable.
        """
        result = execute_plan(
            self,
            ctx=ctx,
            prefer_reader=True,
        )
        if isinstance(result.value, pa.RecordBatchReader):
            return cast("RecordBatchReaderLike", result.value)
        msg = "Plan is not streamable."
        raise ValueError(msg)

    def to_table(self, *, ctx: ExecutionContext) -> TableLike:
        """Materialize the plan as a table.

        Parameters
        ----------
        ctx:
            Execution context controlling threading.

        Returns
        -------
        pyarrow.Table
            Materialized table.
        """
        result = execute_plan(
            self,
            ctx=ctx,
            prefer_reader=False,
        )
        if isinstance(result.value, pa.RecordBatchReader):
            reader = cast("RecordBatchReaderLike", result.value)
            return reader.read_all()
        return cast("TableLike", result.value)

    def schema(self, *, ctx: ExecutionContext) -> SchemaLike:
        """Return the output schema for this plan.

        Parameters
        ----------
        ctx:
            Execution context controlling plan execution.

        Returns
        -------
        pyarrow.Schema
            Output schema for the plan.
        """
        result = execute_plan(
            self,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=False,
        )
        _log_plan_schema(self, value=result.value, ctx=ctx)
        return _schema_from_value(result.value)

    @staticmethod
    def table_source(table: TableLike | RecordBatchReaderLike, *, label: str = "") -> Plan:
        """Create a Plan from an in-memory table or reader.

        Parameters
        ----------
        table:
            Source table or reader.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan backed by a table_source node.
        """
        builder = PlanBuilder()
        builder.table_source(table=table)
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    @staticmethod
    def from_table(table: TableLike, *, label: str = "") -> Plan:
        """Create a Plan from an eager table.

        Parameters
        ----------
        table:
            Source table.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan backed by a table_source node.
        """
        return Plan.table_source(table, label=label)

    @staticmethod
    def from_reader(reader: RecordBatchReaderLike, *, label: str = "") -> Plan:
        """Create a Plan from a reader.

        Parameters
        ----------
        reader:
            Source reader.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan backed by a table_source node.
        """
        return Plan.table_source(reader, label=label)

    def filter(
        self,
        predicate: ComputeExpression,
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add a filter node to the plan.

        Parameters
        ----------
        predicate:
            Predicate expression to apply.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Filtered plan.
        """
        _ = ctx
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.filter(predicate=predicate)
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def project(
        self,
        expressions: Sequence[ComputeExpression],
        names: Sequence[str],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add a project node to the plan.

        Parameters
        ----------
        expressions:
            Expressions to project.
        names:
            Output column names.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Projected plan.
        """
        _ = ctx
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.project(expressions=list(expressions), names=list(names))
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def rename_columns(
        self,
        mapping: Mapping[str, str],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Rename columns using a plan-lane projection.

        Parameters
        ----------
        mapping:
            Mapping of existing column names to new names.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan with renamed columns.

        Raises
        ------
        ValueError
            Raised when no execution context is provided.
        """
        if not mapping:
            return self
        if ctx is None:
            msg = "rename_columns requires an execution context."
            raise ValueError(msg)
        columns = list(self.schema(ctx=ctx).names)
        expressions = [pc.field(name) for name in columns]
        names = [mapping.get(name, name) for name in columns]
        return self.project(expressions, names, ctx=ctx, label=label)

    def order_by(
        self,
        sort_keys: Sequence[OrderingKey],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add an order-by node to the plan.

        Parameters
        ----------
        sort_keys:
            Sort keys as (column, order) pairs.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Ordered plan.
        """
        _ = ctx
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.order_by(sort_keys=tuple(sort_keys))
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def explode_list(
        self,
        *,
        parent_id_col: str,
        list_col: str,
        out_cols: tuple[str, str] = ("src_id", "dst_id"),
        ctx: ExecutionContext,
        label: str = "",
    ) -> Plan:
        """Explode a list column into parent/value pairs.

        Parameters
        ----------
        parent_id_col:
            Column containing parent IDs.
        list_col:
            Column containing list values.
        out_cols:
            Output (parent, value) column names.
        ctx:
            Execution context for plan materialization.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan that explodes the list column.

        Raises
        ------
        ValueError
            Raised when out_cols does not contain exactly two names.
        """
        _ = ctx
        if len(out_cols) != EXPLODE_OUT_COLS_LEN:
            msg = "explode_list out_cols must contain exactly two names."
            raise ValueError(msg)
        out_parent_col, out_value_col = out_cols
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.explode_list(
            parent_id_col=parent_id_col,
            list_col=list_col,
            out_parent_col=out_parent_col,
            out_value_col=out_value_col,
        )
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def aggregate(
        self,
        group_keys: Sequence[str],
        aggs: Sequence[tuple[str, str]],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add an aggregate node to the plan.

        Parameters
        ----------
        group_keys:
            Key columns to group by.
        aggs:
            Aggregates as (column, function) pairs.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Aggregated plan (unordered output).
        """
        _ = ctx
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.aggregate(group_keys=list(group_keys), aggs=list(aggs))
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def winner_select(
        self,
        spec: DedupeSpec,
        columns: Sequence[str],
        *,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Add a winner-select node to the plan.

        Parameters
        ----------
        spec:
            Winner selection specification.
        columns:
            Non-key columns to retain.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan with winner-selection applied.
        """
        _ = ctx
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.winner_select(spec=spec, columns=list(columns))
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def join(
        self,
        right: Plan,
        *,
        spec: JoinSpec,
        ctx: ExecutionContext | None = None,
        label: str = "",
    ) -> Plan:
        """Join this plan with another plan using a hash join.

        Parameters
        ----------
        right:
            Right-hand plan to join against.
        spec:
            Join specification for keys and outputs.
        ctx:
            Optional execution context for plan compilation.
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Joined plan with unordered output.
        """
        _ = ctx
        builder = PlanBuilder.from_plan(
            ir=self.ir,
            ordering=self.ordering,
            pipeline_breakers=self.pipeline_breakers,
        )
        builder.pipeline_breakers.extend(right.pipeline_breakers)
        builder.hash_join(right=right.ir, spec=spec)
        ir, ordering, pipeline_breakers = builder.build()
        return Plan(
            ir=ir,
            label=label or self.label,
            ordering=ordering,
            pipeline_breakers=pipeline_breakers,
        )

    def mark_unordered(self, *, label: str = "") -> Plan:
        """Return a copy of the plan marked unordered.

        Parameters
        ----------
        label:
            Optional plan label.

        Returns
        -------
        Plan
            Plan marked unordered.
        """
        return Plan(
            ir=self.ir,
            label=label or self.label,
            ordering=Ordering.unordered(),
            pipeline_breakers=self.pipeline_breakers,
        )

    def is_ordered(self) -> bool:
        """Return whether this plan is ordered.

        Returns
        -------
        bool
            ``True`` when ordering is implicit or explicit.
        """
        return self.ordering.level in {OrderingLevel.IMPLICIT, OrderingLevel.EXPLICIT}


def _pipeline_breaker_spec(pipeline_breakers: Sequence[str]) -> SchemaMetadataSpec:
    if not pipeline_breakers:
        return SchemaMetadataSpec()
    return SchemaMetadataSpec(
        schema_metadata={b"pipeline_breakers": ",".join(pipeline_breakers).encode("utf-8")}
    )


def _apply_metadata_spec(
    result: TableLike | RecordBatchReaderLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> TableLike | RecordBatchReaderLike:
    if metadata_spec is None:
        return result
    if not metadata_spec.schema_metadata and not metadata_spec.field_metadata:
        return result
    schema = metadata_spec.apply(result.schema)
    if isinstance(result, pa.RecordBatchReader):
        return pa.RecordBatchReader.from_batches(schema, result)
    table = cast("TableLike", result)
    return table.cast(schema)


def _plan_snapshot_path() -> Path | None:
    raw = os.environ.get(_PLAN_SNAPSHOT_ENV, "").strip()
    if not raw:
        return None
    return Path(raw)


def _plan_schema_log_path() -> Path | None:
    raw = os.environ.get(_PLAN_SCHEMA_LOG_ENV, "").strip()
    if not raw:
        return None
    return Path(raw)


def _snapshot_node_args(args: Mapping[str, object]) -> dict[str, str]:
    return {key: type(value).__name__ for key, value in args.items()}


def _snapshot_segments(segmented: SegmentPlan | None) -> list[dict[str, object]]:
    if segmented is None:
        return []
    snapshot: list[dict[str, object]] = []
    for segment in segmented.segments:
        ops = [node.name for node in segment.ops][:_PLAN_SNAPSHOT_OP_LIMIT]
        snapshot.append(
            {
                "lane": segment.lane,
                "op_count": len(segment.ops),
                "ops": ops,
            }
        )
    return snapshot


def _log_plan_snapshot(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    segmented: SegmentPlan | None,
    stage: str,
) -> None:
    path = _plan_snapshot_path()
    if path is None:
        return
    try:
        nodes = plan.ir.nodes
        node_names = [node.name for node in nodes]
        node_samples = node_names[:_PLAN_SNAPSHOT_NODE_LIMIT]
        node_details = [
            {
                "name": node.name,
                "arg_keys": list(node.args.keys()),
                "arg_types": _snapshot_node_args(node.args),
            }
            for node in nodes[:_PLAN_SNAPSHOT_NODE_LIMIT]
        ]
        payload: dict[str, Any] = {
            "timestamp": time.time(),
            "stage": stage,
            "plan_label": plan.label,
            "node_count": len(nodes),
            "node_names": node_samples,
            "nodes": node_details,
            "ordering_level": plan.ordering.level,
            "ordering_keys": list(plan.ordering.keys),
            "pipeline_breakers": list(plan.pipeline_breakers),
            "runtime_profile": ctx.runtime.name,
            "determinism": ctx.runtime.determinism,
            "mode": ctx.mode,
            "provenance": ctx.provenance,
            "safe_cast": ctx.safe_cast,
            "datafusion_enabled": ctx.runtime.datafusion is not None,
            "segments": _snapshot_segments(segmented),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except OSError:
        return


def _schema_from_value(value: TableLike | RecordBatchReaderLike) -> SchemaLike:
    if isinstance(value, pa.RecordBatchReader):
        reader = cast("RecordBatchReaderLike", value)
        return reader.read_all().schema
    table = cast("TableLike", value)
    return table.schema


def _log_plan_schema(plan: Plan, *, value: object, ctx: ExecutionContext) -> None:
    path = _plan_schema_log_path()
    if path is None:
        return
    try:
        payload: dict[str, Any] = {
            "timestamp": time.time(),
            "plan_label": plan.label,
            "node_count": len(plan.ir.nodes),
            "value_type": type(value).__name__,
            "value_module": type(value).__module__,
            "is_table": isinstance(value, pa.Table),
            "is_reader": isinstance(value, pa.RecordBatchReader),
            "runtime_profile": ctx.runtime.name,
            "determinism": ctx.runtime.determinism,
            "use_threads": ctx.use_threads,
            "datafusion_enabled": ctx.runtime.datafusion is not None,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except OSError:
        return


def execute_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_spec: SchemaMetadataSpec | None = None,
    attach_ordering_metadata: bool = True,
) -> PlanRunResult:
    """Execute a Plan via PlanIR segmentation and runtime execution.

    Returns
    -------
    PlanRunResult
        Plan output and materialization kind.

    Raises
    ------
    pyarrow.ArrowIndexError
        Raised when Acero execution fails even after a single-thread retry.
    ValueError
        Raised when the plan requires DataFusion execution.
    """
    plan_ctx = ctx
    if ctx.runtime.datafusion is not None:
        plan_ctx = replace(ctx, runtime=ctx.runtime.with_datafusion(None))
    segmented = segment_plan(plan.ir, catalog=OP_CATALOG, ctx=plan_ctx)
    _log_plan_snapshot(plan, ctx=plan_ctx, segmented=segmented, stage="execute_plan")
    if any(segment.lane == "datafusion" for segment in segmented.segments):
        msg = "DataFusion segments must be executed by the primary pipeline."
        raise ValueError(msg)
    try:
        result = run_segments(segmented, ctx=plan_ctx)
    except pa.ArrowIndexError as exc:
        if not plan_ctx.use_threads:
            raise
        retry_runtime = replace(
            plan_ctx.runtime,
            plan_use_threads=False,
            scan=replace(plan_ctx.runtime.scan, use_threads=False),
        )
        retry_ctx = replace(plan_ctx, runtime=retry_runtime)
        logger.warning(
            "Plan %s failed with threaded Acero execution (%s); retrying single-thread.",
            plan.label,
            exc,
        )
        _log_plan_snapshot(
            plan,
            ctx=retry_ctx,
            segmented=segmented,
            stage="execute_plan_retry_single_thread",
        )
        result = run_segments(segmented, ctx=retry_ctx)
    _log_plan_snapshot(plan, ctx=plan_ctx, segmented=segmented, stage="execute_plan_result")
    if isinstance(result, pa.RecordBatchReader) and prefer_reader:
        combined = metadata_spec
        if attach_ordering_metadata:
            schema_for_ordering = (
                metadata_spec.apply(result.schema) if metadata_spec is not None else result.schema
            )
            ordering_spec = ordering_metadata_for_plan(
                plan.ordering,
                schema=schema_for_ordering,
            )
            combined = merge_metadata_specs(metadata_spec, ordering_spec)
        return PlanRunResult(
            value=_apply_metadata_spec(result, metadata_spec=combined),
            kind="reader",
        )
    if isinstance(result, pa.RecordBatchReader):
        reader = cast("RecordBatchReaderLike", result)
        table = reader.read_all()
    else:
        table = cast("TableLike", result)
    table, canonical_keys = apply_canonical_sort(table, determinism=ctx.determinism)
    _log_plan_snapshot(plan, ctx=plan_ctx, segmented=segmented, stage="execute_plan_sorted")
    combined = metadata_spec
    if attach_ordering_metadata:
        schema_for_ordering = (
            metadata_spec.apply(table.schema) if metadata_spec is not None else table.schema
        )
        ordering_spec = ordering_metadata_for_plan(
            plan.ordering,
            schema=schema_for_ordering,
            canonical_keys=canonical_keys,
        )
        combined = merge_metadata_specs(
            metadata_spec,
            ordering_spec,
            _pipeline_breaker_spec(plan.pipeline_breakers),
        )
    return PlanRunResult(
        value=_apply_metadata_spec(table, metadata_spec=combined),
        kind="table",
    )


def union_all_plans(plans: Sequence[Plan], *, label: str = "") -> Plan:
    """Union multiple plans into a single plan.

    Parameters
    ----------
    plans:
        Plans to union.
    label:
        Optional plan label.

    Returns
    -------
    Plan
        Unioned plan (unordered output).

    Raises
    ------
    ValueError
        Raised when no plans are provided.
    """
    if not plans:
        msg = "union_all_plans requires at least one plan."
        raise ValueError(msg)
    if len(plans) == 1:
        plan = plans[0]
        return Plan(
            ir=plan.ir,
            label=label or plan.label,
            ordering=Ordering.unordered(),
            pipeline_breakers=plan.pipeline_breakers,
        )
    builder = PlanBuilder()
    builder.union_all(inputs=tuple(plan.ir for plan in plans))
    ir, ordering, pipeline_breakers = builder.build()
    combined: list[str] = []
    for plan in plans:
        combined.extend(plan.pipeline_breakers)
    combined.extend(pipeline_breakers)
    return Plan(
        ir=ir,
        label=label,
        ordering=ordering,
        pipeline_breakers=tuple(combined),
    )


def hash_join(*, left: Plan, right: Plan, spec: JoinSpec, label: str = "") -> Plan:
    """Perform a hash join in the plan lane.

    Parameters
    ----------
    left:
        Left-side plan.
    right:
        Right-side plan.
    spec:
        Join specification.
    label:
        Optional plan label.

    Returns
    -------
    Plan
        Joined plan (unordered output).
    """
    return left.join(right, spec=spec, label=label)


__all__ = [
    "Plan",
    "PlanRunResult",
    "PlanSpec",
    "execute_plan",
    "hash_join",
    "union_all_plans",
]
