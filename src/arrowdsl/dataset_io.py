"""Dataset IO helpers for Arrow datasets and Acero plans."""

from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.fs as pafs

from arrowdsl.acero import acero
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import DeclarationLike, SchemaLike, TableLike
from arrowdsl.queryspec import QuerySpec
from arrowdsl.runtime import ExecutionContext

type PathLike = str | Path


def open_dataset(
    path: PathLike,
    *,
    dataset_format: str = "parquet",
    filesystem: pafs.FileSystem | None = None,
    partitioning: str | None = "hive",
    schema: SchemaLike | None = None,
) -> ds.Dataset:
    """Open a dataset for scanning.

    Parameters
    ----------
    path:
        Dataset base path.
    dataset_format:
        Dataset format name (e.g., "parquet").
    filesystem:
        Optional filesystem implementation.
    partitioning:
        Partitioning flavor or ``None``.
    schema:
        Optional dataset schema override.

    Returns
    -------
    pyarrow.dataset.Dataset
        Opened dataset instance.
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

    Parameters
    ----------
    dataset:
        Dataset to scan.
    spec:
        Query specification for projection and predicates.
    ctx:
        Execution context with scan policy.

    Returns
    -------
    pyarrow.dataset.Scanner
        Configured scanner instance.
    """
    return ds.Scanner.from_dataset(
        dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        filter=spec.pushdown_predicate,
        **ctx.runtime.scan.scanner_kwargs(),
    )


def scan_to_table(dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext) -> TableLike:
    """Materialize a dataset scan into a table.

    Parameters
    ----------
    dataset:
        Dataset to scan.
    spec:
        Query specification for projection and predicates.
    ctx:
        Execution context with scan policy.

    Returns
    -------
    pyarrow.Table
        Materialized table.
    """
    scanner = make_scanner(dataset, spec=spec, ctx=ctx)
    return scanner.to_table(use_threads=ctx.scan_use_threads)


def compile_to_acero_scan(
    dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext
) -> DeclarationLike:
    """Compile a scan + filter + project pipeline into an Acero Declaration.

    Parameters
    ----------
    dataset:
        Dataset to scan.
    spec:
        Query specification for projection and predicates.
    ctx:
        Execution context with scan policy.

    Returns
    -------
    pyarrow.acero.Declaration
        Declaration representing the scan pipeline.
    """
    scan_opts = acero.ScanNodeOptions(
        dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        filter=spec.pushdown_predicate,
        **ctx.runtime.scan.scan_node_kwargs(),
    )
    scan = acero.Declaration("scan", scan_opts)

    if spec.predicate is not None:
        scan = acero.Declaration("filter", acero.FilterNodeOptions(spec.predicate), inputs=[scan])

    cols = spec.scan_columns(provenance=ctx.provenance)
    if isinstance(cols, dict):
        proj_exprs = list(cols.values())
        proj_names = list(cols.keys())
    else:
        proj_exprs = [pc.field(col) for col in cols]
        proj_names = list(cols)

    return acero.Declaration(
        "project", acero.ProjectNodeOptions(proj_exprs, proj_names), inputs=[scan]
    )
