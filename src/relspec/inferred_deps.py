"""Dependency inference from SQLGlot expression analysis."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlglot_tools.lineage import referenced_tables
from sqlglot_tools.optimizer import plan_fingerprint

if TYPE_CHECKING:
    import pyarrow as pa

    from sqlglot_tools.compat import Expression



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
    """

    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    required_types: Mapping[str, tuple[tuple[str, str], ...]] = field(default_factory=dict)
    required_metadata: Mapping[str, tuple[tuple[bytes, bytes], ...]] = field(default_factory=dict)
    plan_fingerprint: str = ""


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


def _metadata_from_schema(schema: pa.Schema) -> tuple[tuple[bytes, bytes], ...]:
    if schema.metadata is None:
        return ()
    return tuple(sorted(schema.metadata.items(), key=lambda item: item[0]))


__all__ = [
    "InferredDeps",
    "infer_deps_from_sqlglot_expr",
]
