"""SQLGlot lineage helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from sqlglot import Expression, exp


@dataclass(frozen=True)
class TableRef:
    """Normalized table reference metadata."""

    catalog: str | None
    schema: str | None
    name: str


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


def extract_table_refs(expr: Expression) -> tuple[TableRef, ...]:
    """Return normalized table references for an expression.

    Returns
    -------
    tuple[TableRef, ...]
        Table references extracted from the expression.
    """
    refs: set[TableRef] = set()
    for table in expr.find_all(exp.Table):
        catalog = table.args.get("catalog")
        schema = table.args.get("db")
        catalog_value = str(catalog) if catalog else None
        schema_value = str(schema) if schema else None
        name = table.name
        if name:
            refs.add(TableRef(catalog=catalog_value, schema=schema_value, name=name))
    return tuple(sorted(refs, key=lambda ref: (ref.catalog or "", ref.schema or "", ref.name)))


__all__ = [
    "TableRef",
    "extract_table_refs",
    "iter_table_nodes",
    "referenced_columns",
    "referenced_identifiers",
    "referenced_relations",
    "referenced_tables",
]
