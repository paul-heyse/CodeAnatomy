"""Dataset I/O helpers for Arrow-backed scans."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

from .queryspec import QuerySpec
from .runtime import ExecutionContext

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa
    import pyarrow.dataset as ds
    from pyarrow import acero


PathLike = Union[str, Path]


def open_dataset(
    path: PathLike,
    *,
    format: str = "parquet",
    filesystem: Any = None,
    partitioning: str | None = "hive",
    schema: pa.Schema | None = None,
) -> ds.Dataset:
    """Open a dataset for scanning.

    Returns
    -------
    ds.Dataset
        The Arrow dataset handle.
    """
    import pyarrow.dataset as ds

    return ds.dataset(
        path, format=format, filesystem=filesystem, partitioning=partitioning, schema=schema
    )


def make_scanner(dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext) -> ds.Scanner:
    """Create a dataset scanner under centralized scan policy.

    Returns
    -------
    ds.Scanner
        Configured scanner over the dataset.
    """
    import pyarrow.dataset as ds

    return ds.Scanner.from_dataset(
        dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        filter=spec.pushdown_predicate,
        **ctx.runtime.scan.scanner_kwargs(),
    )


def scan_to_table(dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext) -> pa.Table:
    """Eager scan materialization (primarily for debugging or non-Acero fallback).

    Returns
    -------
    pa.Table
        Materialized scan output.
    """
    scanner = make_scanner(dataset, spec=spec, ctx=ctx)
    return scanner.to_table(use_threads=ctx.scan_use_threads)


def compile_to_acero_scan(
    dataset: ds.Dataset, *, spec: QuerySpec, ctx: ExecutionContext
) -> acero.Declaration:
    """Compile an Acero scan with filter and projection.

    Returns
    -------
    acero.Declaration
        The root declaration for the scan plan.
    """
    import pyarrow.compute as pc
    from pyarrow import acero

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
        proj_exprs = [pc.field(c) for c in cols]
        proj_names = list(cols)

    scan = acero.Declaration(
        "project", acero.ProjectNodeOptions(proj_exprs, proj_names), inputs=[scan]
    )
    return scan
