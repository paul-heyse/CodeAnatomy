"""DataFusion-first view specifications for registry views."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.sql.guard import safe_sql
from schema_spec.specs import TableSchemaSpec
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.sql.guard import SqlBindings


class ViewSchemaMismatchError(ValueError):
    """Raised when a view schema does not match its specification."""


@dataclass(frozen=True)
class ViewSpecInputs:
    """Inputs needed to construct a ViewSpec."""

    ctx: SessionContext
    name: str
    builder: Callable[[SessionContext], DataFrame]
    schema: pa.Schema | None = None


@dataclass(frozen=True)
class ViewSpecSqlInputs:
    """Inputs needed to construct a ViewSpec from SQL."""

    ctx: SessionContext
    name: str
    sql: str
    schema: pa.Schema | None = None
    sql_options: SQLOptions | None = None
    bindings: SqlBindings | None = None


class ViewSpec(StructBaseStrict, frozen=True):
    """Serializable view specification for schema contracts."""

    name: str
    schema: TableSchemaSpec | None = None


def view_spec_from_builder(inputs: ViewSpecInputs) -> ViewSpec:
    """Return a view spec derived from a DataFrame builder.

    Parameters
    ----------
    inputs:
        DataFusion session context used to resolve the view schema.

    Returns
    -------
    ViewSpec
        View specification derived from a DataFusion builder.

    """
    schema = inputs.schema
    if schema is None:
        df = inputs.builder(inputs.ctx)
        schema = _arrow_schema_from_df(df)
    table_spec = TableSchemaSpec.from_schema(inputs.name, schema)
    return ViewSpec(name=inputs.name, schema=table_spec)


def view_spec_from_sql(inputs: ViewSpecSqlInputs) -> ViewSpec:
    """Return a view spec derived from a SQL query.

    Parameters
    ----------
    inputs
        Inputs defining a SQL-backed view.

    Returns
    -------
    ViewSpec
        View specification derived from the SQL query.
    """

    def _builder(session_ctx: SessionContext) -> DataFrame:
        return safe_sql(
            session_ctx,
            inputs.sql,
            sql_options=inputs.sql_options,
            bindings=inputs.bindings,
        )

    return view_spec_from_builder(
        ViewSpecInputs(
            ctx=inputs.ctx,
            name=inputs.name,
            builder=_builder,
            schema=inputs.schema,
        )
    )


def _arrow_schema_from_df(df: DataFrame) -> pa.Schema:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)


__all__ = ["ViewSchemaMismatchError", "ViewSpec", "view_spec_from_builder", "view_spec_from_sql"]
