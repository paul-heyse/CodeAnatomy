"""Derived view helpers for extractor outputs."""

from __future__ import annotations

from arrowdsl.compute.predicates import FilterSpec, InSet
from arrowdsl.core.interop import TableLike


def ast_def_nodes(nodes: TableLike) -> TableLike:
    """Return AST node rows that represent definitions.

    Returns
    -------
    TableLike
        Table filtered to function/class definitions.
    """
    if nodes.num_rows == 0:
        return nodes
    predicate = InSet(
        col="kind",
        values=("FunctionDef", "AsyncFunctionDef", "ClassDef"),
    )
    return FilterSpec(predicate).apply_kernel(nodes)
