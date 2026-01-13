"""Plan scan and row ingestion helpers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs

from arrowdsl.core.context import ExecutionContext, Ordering, execution_context_factory
from arrowdsl.core.interop import (
    ComputeExpression,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.plan.plan import Plan, PlanFactory
from arrowdsl.plan.query import scan_context_factory
from arrowdsl.schema.build import rows_to_table as rows_to_table_factory
from arrowdsl.schema.nested_builders import nested_array_factory
from schema_spec.system import DatasetSpec

if TYPE_CHECKING:
    from arrowdsl.plan.query import QuerySpec


def _record_batch_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    schema: SchemaLike,
) -> pa.RecordBatch:
    arrays = [
        nested_array_factory(field, [row.get(field.name) for row in rows]) for field in schema
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    """Build a table from row mappings or return an empty table.

    Returns
    -------
    TableLike
        Table constructed from rows or an empty table.
    """
    return rows_to_table_factory(rows, schema)


def record_batches_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches from row mappings.

    Yields
    ------
    pyarrow.RecordBatch
        Record batches built from buffered rows.
    """
    buffer: list[Mapping[str, object]] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield _record_batch_from_rows(buffer, schema=schema)
            buffer.clear()
    if buffer:
        yield _record_batch_from_rows(buffer, schema=schema)


def reader_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> RecordBatchReaderLike:
    """Build a RecordBatchReader from row mappings.

    Returns
    -------
    pyarrow.RecordBatchReader
        Reader streaming record batches.
    """
    batches = record_batches_from_rows(rows, schema=schema, batch_size=batch_size)
    return pa.RecordBatchReader.from_batches(schema, batches)


def plan_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
    label: str = "",
) -> Plan:
    """Create a Plan from row mappings via a RecordBatchReader.

    Returns
    -------
    Plan
        Plan backed by a record batch reader.
    """
    reader = reader_from_rows(rows, schema=schema, batch_size=batch_size)
    return Plan.from_reader(reader, label=label)


@dataclass(frozen=True)
class DatasetFactorySpec:
    """Specification for deterministic dataset discovery."""

    root: str
    format: ds.FileFormat
    filesystem: fs.FileSystem | None = None
    partition_base_dir: str | None = None
    exclude_invalid_files: bool = False
    selector_ignore_prefixes: Sequence[str] | None = None
    promote_options: str = "permissive"
    recursive: bool = True

    def build(self, *, schema: SchemaLike | None = None) -> ds.Dataset:
        """Build a dataset using a factory-driven discovery path.

        Returns
        -------
        ds.Dataset
            Dataset instance discovered by the factory.
        """
        filesystem = self.filesystem or fs.LocalFileSystem()
        selector = fs.FileSelector(self.root, recursive=self.recursive)
        options = ds.FileSystemFactoryOptions(
            partition_base_dir=self.partition_base_dir,
            exclude_invalid_files=self.exclude_invalid_files,
            selector_ignore_prefixes=list(self.selector_ignore_prefixes or [".", "_"]),
        )
        factory = ds.FileSystemDatasetFactory(
            filesystem=filesystem,
            paths_or_selector=selector,
            format=self.format,
            options=options,
        )
        inspected = factory.inspect(promote_options=self.promote_options)
        return factory.finish(schema=schema or inspected)


@dataclass(frozen=True)
class ScanSpec:
    """Bundle a dataset source with a query spec."""

    dataset: ds.Dataset | DatasetFactorySpec
    query: QuerySpec

    def open_dataset(self, *, schema: SchemaLike | None = None) -> ds.Dataset:
        """Return a dataset for scanning.

        Returns
        -------
        ds.Dataset
            Dataset instance for scanning.
        """
        if isinstance(self.dataset, DatasetFactorySpec):
            return self.dataset.build(schema=schema)
        return self.dataset

    def to_plan(
        self,
        *,
        ctx: ExecutionContext,
        schema: SchemaLike | None = None,
        label: str = "",
    ) -> Plan:
        """Compile the scan spec into an Acero-backed Plan.

        Returns
        -------
        Plan
            Plan representing the scan/filter/project pipeline.
        """
        dataset = self.open_dataset(schema=schema)
        return self.query.to_plan(
            dataset=dataset,
            ctx=ctx,
            label=label,
            scan_provenance=ctx.runtime.scan.scan_provenance_columns,
        )


@dataclass(frozen=True)
class DatasetSource:
    """Dataset + DatasetSpec pairing for plan compilation."""

    dataset: ds.Dataset
    spec: DatasetSpec


@dataclass(frozen=True)
class InMemoryDatasetSource:
    """RecordBatchReader source with one-shot semantics."""

    reader: RecordBatchReaderLike
    one_shot: bool = True


@dataclass(frozen=True)
class RowSource:
    """Row iterator source with an explicit schema."""

    rows: Iterable[Mapping[str, object]]
    schema: SchemaLike
    batch_size: int = 4096


PlanSource = (
    TableLike
    | RecordBatchReaderLike
    | ds.Dataset
    | ds.Scanner
    | DatasetSource
    | DatasetFactorySpec
    | ScanSpec
    | InMemoryDatasetSource
    | RowSource
    | Plan
)


def plan_from_dataset(
    dataset: ds.Dataset,
    *,
    spec: DatasetSpec,
    ctx: ExecutionContext,
) -> Plan:
    """Compile a dataset-backed scan plan with ordering metadata.

    Returns
    -------
    Plan
        Plan representing the dataset scan and projection.
    """
    scan_ctx = spec.scan_context(dataset, ctx)
    ordering = (
        Ordering.implicit()
        if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
        else Ordering.unordered()
    )
    return Plan(
        decl=scan_ctx.acero_decl(),
        label=spec.name,
        ordering=ordering,
        pipeline_breakers=(),
    )


def _plan_from_scan_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None,
    label: str,
) -> Plan | None:
    if isinstance(source, DatasetSource):
        return plan_from_dataset(source.dataset, spec=source.spec, ctx=ctx)
    if isinstance(source, ScanSpec):
        scan_ctx = scan_context_factory(source, ctx=ctx)
        return scan_ctx.to_plan(label=label)
    if isinstance(source, DatasetFactorySpec):
        return plan_from_source(
            source.build(),
            ctx=ctx,
            columns=columns,
            label=label,
        )
    if isinstance(source, ds.Dataset):
        dataset = cast("ds.Dataset", source)
        available = set(dataset.schema.names)
        scan_cols = list(columns) if columns is not None else list(dataset.schema.names)
        scan_cols = [name for name in scan_cols if name in available]
        for name in ctx.runtime.scan.scan_provenance_columns:
            if name not in scan_cols:
                scan_cols.append(name)
        factory = PlanFactory(ctx=ctx)
        return factory.scan(dataset, columns=scan_cols, label=label)
    if isinstance(source, ds.Scanner):
        scanner = cast("ds.Scanner", source)
        return Plan.from_reader(scanner.to_reader(), label=label)
    return None


def _plan_from_reader_source(
    source: PlanSource,
    *,
    label: str,
) -> Plan | None:
    if isinstance(source, InMemoryDatasetSource):
        if source.one_shot:
            return Plan.table_source(source.reader.read_all(), label=label)
        return Plan.from_reader(source.reader, label=label)
    if isinstance(source, RowSource):
        return plan_from_rows(
            source.rows,
            schema=source.schema,
            batch_size=source.batch_size,
            label=label,
        )
    if isinstance(source, RecordBatchReaderLike):
        return Plan.from_reader(source, label=label)
    return None


def plan_from_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    """Return a plan for tables, readers, or dataset-backed sources.

    Returns
    -------
    Plan
        Acero-backed plan for dataset/table sources.
    """
    if isinstance(source, Plan):
        return source
    plan = _plan_from_scan_source(source, ctx=ctx, columns=columns, label=label)
    if plan is not None:
        return plan
    plan = _plan_from_reader_source(source, label=label)
    if plan is not None:
        return plan
    return Plan.table_source(cast("TableLike", source), label=label)


def plan_from_source_profile(
    source: PlanSource,
    *,
    profile: str,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    """Return a plan for a source using a named execution profile.

    Returns
    -------
    Plan
        Plan for the source with the named profile context.
    """
    ctx = execution_context_factory(profile)
    return plan_from_source(source, ctx=ctx, columns=columns, label=label)


def plan_from_scan_columns(
    dataset: ds.Dataset,
    *,
    columns: Sequence[str] | Mapping[str, ComputeExpression],
    ctx: ExecutionContext,
    label: str = "",
) -> Plan:
    """Build a scan plan from explicit columns.

    Returns
    -------
    Plan
        Plan built from a scan declaration.
    """
    factory = PlanFactory(ctx=ctx)
    return factory.scan(dataset, columns=columns, label=label)


__all__ = [
    "DatasetFactorySpec",
    "DatasetSource",
    "InMemoryDatasetSource",
    "PlanSource",
    "RowSource",
    "ScanSpec",
    "plan_from_dataset",
    "plan_from_rows",
    "plan_from_scan_columns",
    "plan_from_source",
    "plan_from_source_profile",
    "reader_from_rows",
    "record_batches_from_rows",
    "rows_to_table",
]
