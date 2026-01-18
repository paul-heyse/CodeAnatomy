"""SQLGlot lineage helpers for Ibis expressions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from ibis.expr.types import Table as IbisTable
from sqlglot.lineage import Node, lineage

from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot

SchemaMapping = Mapping[str, Mapping[str, str]]


def required_columns_by_table(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema_map: SchemaMapping | None = None,
    dialect: str = "datafusion",
) -> dict[str, tuple[str, ...]]:
    """Return required source columns per table for an expression.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of table name to required column names.
    """
    sg_expr = ibis_to_sqlglot(expr, backend=backend, params=None)
    schema = {name: dict(cols) for name, cols in schema_map.items()} if schema_map else None
    required: dict[str, set[str]] = {}
    output_columns = tuple(cast("tuple[str, ...]", expr.schema().names))
    for column in output_columns:
        node = lineage(column, sg_expr, schema=schema, dialect=dialect)
        _collect_required_columns(node, required)
    return {table: tuple(sorted(cols)) for table, cols in required.items()}


def lineage_graph_by_output(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema_map: SchemaMapping | None = None,
    dialect: str = "datafusion",
) -> dict[str, tuple[str, ...]]:
    """Return output-to-source lineage entries for an expression.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of output column name to source column references.
    """
    sg_expr = ibis_to_sqlglot(expr, backend=backend, params=None)
    schema = {name: dict(cols) for name, cols in schema_map.items()} if schema_map else None
    lineage_map: dict[str, tuple[str, ...]] = {}
    for column in tuple(cast("tuple[str, ...]", expr.schema().names)):
        node = lineage(column, sg_expr, schema=schema, dialect=dialect)
        sources = sorted(_collect_leaf_names(node))
        if sources:
            lineage_map[column] = tuple(sources)
    return lineage_map


def _collect_required_columns(node: Node, required: dict[str, set[str]]) -> None:
    if node.downstream:
        for child in node.downstream:
            _collect_required_columns(child, required)
        return
    table, column = _split_table_column(node.name)
    if table is None or column is None:
        return
    required.setdefault(table, set()).add(column)


def _split_table_column(name: str) -> tuple[str | None, str | None]:
    if not name:
        return None, None
    if "." not in name:
        return None, None
    table, column = name.rsplit(".", maxsplit=1)
    if not table or not column:
        return None, None
    return table, column


def _collect_leaf_names(node: Node) -> set[str]:
    if node.downstream:
        leaves: set[str] = set()
        for child in node.downstream:
            leaves.update(_collect_leaf_names(child))
        return leaves
    return {node.name} if node.name else set()


__all__ = ["lineage_graph_by_output", "required_columns_by_table"]
