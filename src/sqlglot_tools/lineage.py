"""SQLGlot lineage helpers."""

from __future__ import annotations

import contextlib
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope
from sqlglot.serde import dump

from sqlglot_tools.compat import Expression, exp


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


@dataclass(frozen=True)
class LineagePayload:
    """Lineage metadata for a SQL expression."""

    tables: tuple[str, ...]
    columns: tuple[str, ...]
    scopes: tuple[str, ...]
    canonical_fingerprint: str
    qualified_sql: str | None = None


@dataclass(frozen=True)
class LineageExtractionOptions:
    """Options for lineage extraction."""

    schema: Mapping[str, Mapping[str, str]] | None = None
    dialect: str | None = None
    include_qualified_sql: bool = False


def extract_lineage_payload(
    expr: Expression,
    *,
    options: LineageExtractionOptions | None = None,
) -> LineagePayload:
    """Extract lineage metadata from SQLGlot expression.

    Uses SQLGlot's qualify and scope analysis to extract comprehensive
    lineage information including tables, columns, and scopes.

    Parameters
    ----------
    expr : Expression
        SQLGlot expression to analyze.
    options : LineageExtractionOptions | None
        Extraction options including schema for qualification.

    Returns
    -------
    LineagePayload
        Lineage metadata with tables, columns, scopes, and fingerprint.
    """
    resolved = options or LineageExtractionOptions()

    # Qualify the expression for accurate lineage
    qualified = expr
    if resolved.schema is not None:
        schema_mapping = {key: dict(value) for key, value in resolved.schema.items()}
        try:
            qualified = qualify(
                expr,
                schema=schema_mapping,
                dialect=resolved.dialect,
            )
        except (SqlglotError, TypeError, ValueError):
            # Qualification may fail for complex expressions
            qualified = expr

    # Extract tables
    tables = referenced_tables(qualified)

    # Extract columns
    columns = referenced_columns(qualified)

    # Build scope info
    scopes: list[str] = []
    try:
        root_scope = build_scope(qualified)
    except (SqlglotError, TypeError, ValueError):
        root_scope = None
    if root_scope is not None:
        for scope in root_scope.traverse():
            scope_type = type(scope).__name__
            scope_tables = [str(t.name) for t in scope.sources.values() if hasattr(t, "name")]
            scopes.append(f"{scope_type}:{','.join(scope_tables)}")

    # Canonical fingerprint from AST serialization
    fingerprint = canonical_ast_fingerprint(qualified)

    # Optional qualified SQL
    qualified_sql = None
    if resolved.include_qualified_sql:
        with contextlib.suppress(SqlglotError, TypeError, ValueError):
            qualified_sql = qualified.sql(dialect=resolved.dialect or "datafusion")

    return LineagePayload(
        tables=tables,
        columns=columns,
        scopes=tuple(scopes),
        canonical_fingerprint=fingerprint,
        qualified_sql=qualified_sql,
    )


def canonical_ast_fingerprint(expr: Expression) -> str:
    """Generate stable fingerprint for canonical AST.

    Uses SQLGlot's serde.dump to serialize the AST to JSON,
    then hashes for a stable fingerprint.

    Parameters
    ----------
    expr : Expression
        SQLGlot expression to fingerprint.

    Returns
    -------
    str
        SHA256 hex digest of the canonical AST.
    """
    try:
        payload = dump(expr)
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()
    except (SqlglotError, TypeError, ValueError):
        # Fallback to SQL text hash
        try:
            sql = expr.sql()
            return hashlib.sha256(sql.encode()).hexdigest()
        except (SqlglotError, TypeError, ValueError):
            return hashlib.sha256(repr(expr).encode()).hexdigest()


__all__ = [
    "LineageExtractionOptions",
    "LineagePayload",
    "TableRef",
    "canonical_ast_fingerprint",
    "extract_lineage_payload",
    "extract_table_refs",
    "iter_table_nodes",
    "referenced_columns",
    "referenced_identifiers",
    "referenced_relations",
    "referenced_tables",
]
