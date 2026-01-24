"""Dependency inference from Ibis/SQLGlot expression analysis.

This module provides calculation-driven dependency inference as an alternative
to declared-dependency scheduling. The dependency graph is derived from actual
Ibis/DataFusion expression analysis rather than bespoke task specifications.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlglot.errors import SqlglotError

from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.lineage import referenced_tables, required_columns_by_table
from sqlglot_tools.optimizer import plan_fingerprint

if TYPE_CHECKING:
    import pyarrow as pa

    from ibis_engine.plan import IbisPlan
    from sqlglot_tools.compat import Expression

_LOG = logging.getLogger(__name__)
_SCHEMA_SUFFIX_PARTS = 2


@dataclass(frozen=True)
class InferredDeps:
    """Dependencies inferred from Ibis/SQLGlot expression analysis.

    Captures table and column-level dependencies by analyzing the actual
    query plan rather than relying on declared inputs.

    Attributes
    ----------
    task_name : str
        Name of the task these dependencies apply to.
    output : str
        Output dataset name produced by the task.
    inputs : tuple[str, ...]
        Table names inferred from expression analysis.
    required_columns : Mapping[str, tuple[str, ...]]
        Per-table columns required by the expression.
    required_types : Mapping[str, tuple[tuple[str, str], ...]]
        Per-table required column/type pairs.
    required_metadata : Mapping[str, tuple[tuple[bytes, bytes], ...]]
        Per-table required metadata entries.
    plan_fingerprint : str
        Stable hash for caching and comparison.
    plan_fingerprint : str
        Stable hash for caching and comparison.
    """

    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    required_types: Mapping[str, tuple[tuple[str, str], ...]] = field(default_factory=dict)
    required_metadata: Mapping[str, tuple[tuple[bytes, bytes], ...]] = field(default_factory=dict)
    plan_fingerprint: str = ""


@dataclass(frozen=True)
class InferredDepsRequest:
    """Inputs for dependency inference."""

    task_name: str
    output: str
    dialect: str = "datafusion"


def infer_deps_from_ibis_plan(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    request: InferredDepsRequest,
    sqlglot_expr: Expression | None = None,
) -> InferredDeps:
    """Infer dependencies from an Ibis plan using SQLGlot lineage.

    Analyzes the Ibis expression to extract:
    - Referenced tables (input dependencies)
    - Required columns per table (column-level lineage)
    - Plan fingerprint for caching

    Parameters
    ----------
    plan : IbisPlan
        Compiled Ibis plan to analyze.
    backend : IbisCompilerBackend
        Ibis backend with SQLGlot compiler.
    request : InferredDepsRequest
        Request payload including task name, output, and optional declared inputs.
    sqlglot_expr : Expression | None
        Optional precompiled SQLGlot expression for the plan.

    Returns
    -------
    InferredDeps
        Inferred dependencies with comparison metadata.
    """
    task_name = request.task_name
    output = request.output
    dialect = request.dialect

    # Compile Ibis to SQLGlot for analysis
    sg_expr = sqlglot_expr or ibis_to_sqlglot(plan.expr, backend=backend, params=None)

    # Extract table references
    tables = referenced_tables(sg_expr)

    # Extract column-level lineage
    columns_by_table: dict[str, tuple[str, ...]] = {}
    try:
        columns_by_table = required_columns_by_table(
            plan.expr,
            backend=backend,
            dialect=dialect,
        )
    except (SqlglotError, TypeError, ValueError):
        _LOG.debug(
            "Column-level lineage extraction failed for task %r, using table-level only",
            task_name,
            exc_info=True,
        )

    required_types = _required_types_for_tables(columns_by_table, backend=backend)
    required_metadata = _required_metadata_for_tables(columns_by_table)

    # Compute plan fingerprint
    fingerprint = plan_fingerprint(sg_expr, dialect=dialect)

    return InferredDeps(
        task_name=task_name,
        output=output,
        inputs=tables,
        required_columns=columns_by_table,
        required_types=required_types,
        required_metadata=required_metadata,
        plan_fingerprint=fingerprint,
    )


def infer_deps_from_sqlglot_expr(
    expr: object,
    *,
    task_name: str,
    output: str,
    dialect: str = "datafusion",
) -> InferredDeps:
    """Infer dependencies from a raw SQLGlot expression.

    Lower-level variant that works directly with SQLGlot expressions
    when an Ibis plan is not available.

    Parameters
    ----------
    expr : Expression
        SQLGlot expression to analyze.
    task_name : str
        Name of the task being analyzed.
    output : str
        Output dataset name.
    dialect : str
        SQL dialect for fingerprinting.

    Returns
    -------
    InferredDeps
        Inferred dependencies.

    Raises
    ------
    TypeError
        Raised when expr is not a SQLGlot Expression.
    """
    from sqlglot_tools.compat import Expression

    if not isinstance(expr, Expression):
        msg = f"Expected SQLGlot Expression, got {type(expr).__name__}"
        raise TypeError(msg)

    # Extract table references
    tables = referenced_tables(expr)
    columns_by_table = _required_columns_from_sqlglot(expr)

    required_types = _required_types_from_registry(columns_by_table)
    required_metadata = _required_metadata_for_tables(columns_by_table)

    # Compute plan fingerprint
    fingerprint = plan_fingerprint(expr, dialect=dialect)

    return InferredDeps(
        task_name=task_name,
        output=output,
        inputs=tables,
        required_columns=columns_by_table,
        required_types=required_types,
        required_metadata=required_metadata,
        plan_fingerprint=fingerprint,
    )


def _required_types_for_tables(
    columns_by_table: Mapping[str, tuple[str, ...]],
    *,
    backend: IbisCompilerBackend,
) -> dict[str, tuple[tuple[str, str], ...]]:
    required: dict[str, tuple[tuple[str, str], ...]] = {}
    if not columns_by_table:
        return required
    schema_lookup = _schema_lookup_map(backend)
    for table_name, columns in columns_by_table.items():
        schema = _schema_for_table(table_name)
        if schema is not None:
            pairs = _types_from_schema(schema, columns)
        else:
            pairs = _types_from_lookup(schema_lookup, table_name, columns)
        if pairs:
            required[table_name] = pairs
    return required


def _required_metadata_for_tables(
    columns_by_table: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[tuple[bytes, bytes], ...]]:
    required: dict[str, tuple[tuple[bytes, bytes], ...]] = {}
    for table_name in columns_by_table:
        schema = _schema_for_table(table_name)
        if schema is None:
            continue
        metadata = _metadata_from_schema(schema)
        if metadata:
            required[table_name] = metadata
    return required


def _required_columns_from_sqlglot(
    expr: Expression,
) -> dict[str, tuple[str, ...]]:
    from sqlglot_tools.compat import exp

    required: dict[str, set[str]] = {}
    for column in expr.find_all(exp.Column):
        table = column.table
        if not table:
            continue
        required.setdefault(table, set()).add(column.name)
    return {table: tuple(sorted(cols)) for table, cols in required.items()}


def _required_types_from_registry(
    columns_by_table: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[tuple[str, str], ...]]:
    required: dict[str, tuple[tuple[str, str], ...]] = {}
    for table_name, columns in columns_by_table.items():
        schema = _schema_for_table(table_name)
        if schema is None:
            continue
        pairs = _types_from_schema(schema, columns)
        if pairs:
            required[table_name] = pairs
    return required


def _schema_for_table(name: str) -> pa.Schema | None:
    try:
        from datafusion_engine.schema_registry import schema_for
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    try:
        return schema_for(name)
    except KeyError:
        return None


def _schema_lookup_map(
    backend: IbisCompilerBackend,
) -> Mapping[str, Mapping[str, str]]:
    try:
        from datafusion_engine.runtime import sql_options_for_profile
        from datafusion_engine.schema_introspection import SchemaIntrospector
        from ibis_engine.registry import datafusion_context
    except (ImportError, RuntimeError, TypeError, ValueError):
        return {}
    try:
        ctx = datafusion_context(backend)
    except (TypeError, ValueError):
        return {}
    introspector = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None))
    schema_map = introspector.schema_map()
    return _expand_schema_lookup(schema_map)


def _expand_schema_lookup(
    schema_map: Mapping[str, Mapping[str, str]],
) -> dict[str, Mapping[str, str]]:
    lookup: dict[str, Mapping[str, str]] = {}
    for table_key, columns in schema_map.items():
        lookup.setdefault(table_key, columns)
        parts = table_key.split(".")
        if parts:
            lookup.setdefault(parts[-1], columns)
        if len(parts) >= _SCHEMA_SUFFIX_PARTS:
            lookup.setdefault(".".join(parts[-2:]), columns)
    return lookup


def _types_from_schema(
    schema: pa.Schema,
    columns: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for name in columns:
        if name in schema.names:
            dtype = schema.field(name).type
            pairs.append((name, str(dtype)))
    return tuple(pairs)


def _types_from_lookup(
    lookup: Mapping[str, Mapping[str, str]],
    table_name: str,
    columns: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    columns_map = lookup.get(table_name)
    if columns_map is None:
        return ()
    pairs: list[tuple[str, str]] = []
    for name in columns:
        dtype = columns_map.get(name)
        if dtype is not None:
            pairs.append((name, str(dtype)))
    return tuple(pairs)


def _metadata_from_schema(schema: pa.Schema) -> tuple[tuple[bytes, bytes], ...]:
    if schema.metadata is None:
        return ()
    return tuple(sorted(schema.metadata.items(), key=lambda item: item[0]))


__all__ = [
    "InferredDeps",
    "InferredDepsRequest",
    "infer_deps_from_ibis_plan",
    "infer_deps_from_sqlglot_expr",
]
