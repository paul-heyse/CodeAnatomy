"""SQLGlot lineage helpers."""

from __future__ import annotations

from collections.abc import Iterable

from sqlglot import Expression, exp


def referenced_tables(expr: Expression) -> tuple[str, ...]:
    """Return referenced table names in the expression.

    Returns
    -------
    tuple[str, ...]
        Table names referenced by the expression.
    """
    tables = {table.name for table in expr.find_all(exp.Table)}
    return tuple(sorted(tables))


def referenced_columns(expr: Expression) -> tuple[str, ...]:
    """Return referenced column names in the expression.

    Returns
    -------
    tuple[str, ...]
        Column names referenced by the expression.
    """
    columns = {column.alias_or_name for column in expr.find_all(exp.Column)}
    return tuple(sorted(columns))


def referenced_identifiers(expr: Expression) -> tuple[str, ...]:
    """Return identifiers referenced by the expression.

    Returns
    -------
    tuple[str, ...]
        Identifiers referenced by the expression.
    """
    ids: set[str] = set()
    for node in expr.find_all(exp.Identifier):
        if node.this:
            ids.add(node.this)
    return tuple(sorted(ids))


def referenced_relations(expr: Expression) -> dict[str, tuple[str, ...]]:
    """Return referenced tables and columns for the expression.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of relation type to referenced names.
    """
    return {
        "tables": referenced_tables(expr),
        "columns": referenced_columns(expr),
        "identifiers": referenced_identifiers(expr),
    }


def iter_table_nodes(expr: Expression) -> Iterable[exp.Table]:
    """Yield table nodes from an expression.

    Yields
    ------
    sqlglot.expressions.Table
        Table nodes encountered in the expression.
    """
    yield from expr.find_all(exp.Table)
