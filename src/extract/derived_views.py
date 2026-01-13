"""Derived view helpers for extractor outputs."""

from __future__ import annotations

from arrowdsl.compute.predicates import FilterSpec, predicate_spec
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.plan import Plan


def ast_def_nodes(nodes: TableLike) -> TableLike:
    """Return AST node rows that represent definitions.

    Returns
    -------
    TableLike
        Table filtered to function/class definitions.
    """
    if nodes.num_rows == 0:
        return nodes
    predicate = predicate_spec(
        "in_set",
        col="kind",
        values=("FunctionDef", "AsyncFunctionDef", "ClassDef"),
    )
    return FilterSpec(predicate).apply_kernel(nodes)


def ast_def_nodes_plan(plan: Plan) -> Plan:
    """Return a plan filtered to AST definition nodes.

    Returns
    -------
    Plan
        Plan filtered to function/class definitions.
    """
    predicate = predicate_spec(
        "in_set",
        col="kind",
        values=("FunctionDef", "AsyncFunctionDef", "ClassDef"),
    )
    return FilterSpec(predicate).apply_plan(plan)
