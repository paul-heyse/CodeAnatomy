"""SQLGlot lineage helpers."""

from __future__ import annotations

import contextlib
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

from sqlglot.errors import SqlglotError
from sqlglot.lineage import Node, lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope
from sqlglot.schema import MappingSchema

from datafusion_engine.sql_policy_engine import SQLPolicyProfile, compile_sql_policy
from sqlglot_tools.compat import Expression, exp
from sqlglot_tools.optimizer import (
    SchemaMapping,
    SchemaMappingNode,
    SqlGlotPolicy,
    canonical_ast_fingerprint,
    default_sqlglot_policy,
    schema_map_fingerprint_from_mapping,
)

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from sqlglot_tools.bridge import IbisCompilerBackend


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


def referenced_udf_calls(expr: Expression) -> tuple[str, ...]:
    """Return referenced UDF-style function names in the expression.

    Returns
    -------
    tuple[str, ...]
        UDF-style function names referenced by the expression.
    """
    names: set[str] = set()
    for node in expr.find_all(exp.Anonymous):
        name = node.name
        if isinstance(name, str) and name:
            names.add(name)
    return tuple(sorted(names))


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


_SCOPE_CACHE_MAX: int = 256
_SCOPE_CACHE: OrderedDict[tuple[str, str | None], Scope] = OrderedDict()


def _serde_fingerprint(expr: Expression) -> str:
    """Return a stable serde-based fingerprint for the AST.

    Returns
    -------
    str
        Stable fingerprint for the AST.
    """
    return canonical_ast_fingerprint(expr)


def _schema_hash(schema_map: SchemaMapping | None) -> str | None:
    if schema_map is None:
        return None
    return schema_map_fingerprint_from_mapping(schema_map)


def _cached_scope(
    expr: Expression,
    *,
    schema_map: SchemaMapping | None,
) -> Scope | None:
    key = (_serde_fingerprint(expr), _schema_hash(schema_map))
    cached = _SCOPE_CACHE.get(key)
    if cached is not None:
        _SCOPE_CACHE.move_to_end(key)
        return cached
    try:
        scope = cast("Scope", build_scope(expr))
    except (SqlglotError, TypeError, ValueError):
        return None
    _SCOPE_CACHE[key] = scope
    if len(_SCOPE_CACHE) > _SCOPE_CACHE_MAX:
        _SCOPE_CACHE.popitem(last=False)
    return scope


def build_scope_cached(
    expr: Expression,
    *,
    schema_map: SchemaMapping | None = None,
) -> Scope | None:
    """Return a cached SQLGlot scope for an expression.

    Returns
    -------
    sqlglot.optimizer.scope.Scope | None
        Cached scope instance when available.
    """
    return _cached_scope(expr, schema_map=schema_map)


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
    root_scope = _cached_scope(qualified, schema_map=resolved.schema)
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


def required_columns_by_table(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema_map: SchemaMapping | None = None,
    dialect: str = "datafusion",
    policy: SqlGlotPolicy | None = None,
) -> dict[str, tuple[str, ...]]:
    """Return required source columns per table for an expression.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of table name to required column names.
    """
    from sqlglot_tools.bridge import ibis_to_sqlglot

    sg_expr = ibis_to_sqlglot(expr, backend=backend, params=None)
    schema_map_resolved = schema_map or _schema_map_from_backend(backend)
    compile_schema = schema_map_resolved or MappingSchema({})
    policy = policy or default_sqlglot_policy()
    if dialect:
        policy = replace(policy, read_dialect=dialect, write_dialect=dialect)
    normalized, _ = compile_sql_policy(
        sg_expr,
        schema=compile_schema,
        profile=SQLPolicyProfile(
            policy=policy,
            read_dialect=policy.read_dialect,
            write_dialect=policy.write_dialect,
        ),
    )
    scoped = _cached_scope(normalized, schema_map=schema_map_resolved)
    if scoped is None:
        return {}
    required: dict[str, set[str]] = {}
    _collect_expression_columns(normalized, required)
    output_columns = tuple(cast("tuple[str, ...]", expr.schema().names))
    lineage_schema = _schema_for_lineage(schema_map_resolved)
    for column in output_columns:
        node = lineage(
            column,
            normalized,
            schema=lineage_schema,
            dialect=dialect,
            scope=scoped,
            trim_selects=True,
            copy=False,
        )
        _collect_required_columns(node, required)
    return {table: tuple(sorted(cols)) for table, cols in required.items()}


def lineage_graph_by_output(
    expr: IbisTable,
    *,
    backend: IbisCompilerBackend,
    schema_map: SchemaMapping | None = None,
    dialect: str = "datafusion",
    policy: SqlGlotPolicy | None = None,
) -> dict[str, tuple[str, ...]]:
    """Return output-to-source lineage entries for an expression.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of output column name to source column references.
    """
    from sqlglot_tools.bridge import ibis_to_sqlglot

    sg_expr = ibis_to_sqlglot(expr, backend=backend, params=None)
    schema_map_resolved = schema_map or _schema_map_from_backend(backend)
    compile_schema = schema_map_resolved or MappingSchema({})
    policy = policy or default_sqlglot_policy()
    if dialect:
        policy = replace(policy, read_dialect=dialect, write_dialect=dialect)
    normalized, _ = compile_sql_policy(
        sg_expr,
        schema=compile_schema,
        profile=SQLPolicyProfile(
            policy=policy,
            read_dialect=policy.read_dialect,
            write_dialect=policy.write_dialect,
        ),
    )
    scoped = _cached_scope(normalized, schema_map=schema_map_resolved)
    if scoped is None:
        return {}
    lineage_map: dict[str, tuple[str, ...]] = {}
    lineage_schema = _schema_for_lineage(schema_map_resolved)
    for column in tuple(cast("tuple[str, ...]", expr.schema().names)):
        node = lineage(
            column,
            normalized,
            schema=lineage_schema,
            dialect=dialect,
            scope=scoped,
            trim_selects=True,
            copy=False,
        )
        sources = sorted(_collect_leaf_names(node))
        if sources:
            lineage_map[column] = tuple(sources)
    return lineage_map


def _collect_expression_columns(expr: exp.Expression, required: dict[str, set[str]]) -> None:
    for column in expr.find_all(exp.Column):
        table = column.table
        name = column.name
        if not table or not name:
            continue
        required.setdefault(table, set()).add(name)


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


def _schema_map_from_backend(backend: IbisCompilerBackend) -> dict[str, dict[str, str]] | None:
    from datafusion_engine.runtime import sql_options_for_profile
    from ibis_engine.registry import datafusion_context

    try:
        ctx = datafusion_context(backend)
    except (TypeError, ValueError):
        return None
    from datafusion_engine.schema_introspection import SchemaIntrospector

    return SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None)).schema_map()


def _schema_for_lineage(schema: SchemaMapping | None) -> dict[str, object] | None:
    if schema is None:
        return None
    return _schema_node_to_dict(schema)


def _schema_node_to_dict(node: SchemaMappingNode) -> dict[str, object]:
    mapped: dict[str, object] = {}
    for key, value in node.items():
        if isinstance(value, Mapping):
            mapped[str(key)] = _schema_node_to_dict(value)
        else:
            mapped[str(key)] = value
    return mapped


__all__ = [
    "LineageExtractionOptions",
    "LineagePayload",
    "TableRef",
    "build_scope_cached",
    "canonical_ast_fingerprint",
    "extract_lineage_payload",
    "extract_table_refs",
    "iter_table_nodes",
    "lineage_graph_by_output",
    "referenced_columns",
    "referenced_identifiers",
    "referenced_relations",
    "referenced_tables",
    "referenced_udf_calls",
    "required_columns_by_table",
]
