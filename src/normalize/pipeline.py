"""Minimal plan execution helpers for normalization pipelines."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import pyarrow as pa
from pyarrow import acero

from arrowdsl.compute.kernels import canonical_sort as _canonical_sort
from arrowdsl.plan.ops import SortKey


def canonical_sort(
    table: pa.Table,
    sort_keys: Sequence[tuple[str, Literal["ascending", "descending"]]],
) -> pa.Table:
    """Return a canonically sorted table using stable sort indices.

    Returns
    -------
    pa.Table
        Sorted table.
    """
    keys = [SortKey(column=col, order=order) for col, order in sort_keys]
    return _canonical_sort(table, sort_keys=keys)


def run_plan(decl: acero.Declaration, *, use_threads: bool) -> pa.Table:
    """Execute an Acero declaration as a table.

    Returns
    -------
    pa.Table
        Materialized table from the declaration.
    """
    return decl.to_table(use_threads=use_threads)


__all__ = ["canonical_sort", "run_plan"]
