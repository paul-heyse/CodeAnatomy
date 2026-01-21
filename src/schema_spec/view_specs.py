"""Schema-first view specifications for DataFusion integration."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext
from sqlglot import exp

from datafusion_engine.schema_introspection import SchemaIntrospector
from sqlglot_tools.optimizer import (
    default_sqlglot_policy,
    normalize_ddl_sql,
    parse_sql_strict,
    register_datafusion_dialect,
    sqlglot_sql,
)


class ViewSchemaMismatchError(ValueError):
    """Raised when a view schema does not match its specification."""


def _schema_signature(schema: pa.Schema) -> tuple[tuple[str, pa.DataType, bool], ...]:
    """Return a simplified schema signature for comparisons.

    Returns
    -------
    tuple[tuple[str, pyarrow.DataType, bool], ...]
        Column name, data type, and nullability signatures.
    """
    return tuple((field.name, field.type, field.nullable) for field in schema)


def view_spec_from_sql(ctx: SessionContext, *, name: str, sql: str) -> ViewSpec:
    """Return a view spec derived from DataFusion schema inference.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer the schema.
    name:
        Name to assign to the view spec.
    sql:
        SQL statement used to infer the schema.

    Returns
    -------
    ViewSpec
        View specification with the inferred schema.
    """
    schema = ctx.sql(sql).schema()
    return ViewSpec(name=name, sql=sql, schema=schema)


@dataclass(frozen=True)
class ViewSpec:
    """Schema-validated view definition."""

    name: str
    sql: str
    schema: pa.Schema

    def create_view_sql(self) -> str:
        """Return a CREATE OR REPLACE VIEW statement for the view.

        Returns
        -------
        str
            SQL statement that creates or replaces the view.
        """
        policy = default_sqlglot_policy()
        register_datafusion_dialect()
        query = parse_sql_strict(self.sql, dialect=policy.read_dialect)
        create_expr = exp.Create(
            this=exp.Table(this=exp.Identifier(this=self.name)),
            kind="VIEW",
            replace=True,
            expression=query,
        )
        ddl = sqlglot_sql(create_expr, policy=policy)
        return normalize_ddl_sql(ddl)

    def describe(
        self,
        ctx: SessionContext,
        introspector: SchemaIntrospector | None = None,
    ) -> list[dict[str, object]]:
        """Return DESCRIBE rows for the view's query.

        Parameters
        ----------
        ctx:
            DataFusion session context used for DESCRIBE execution.
        introspector:
            Optional schema introspector to reuse existing context state.

        Returns
        -------
        list[dict[str, object]]
            ``DESCRIBE`` output rows for the view query.
        """
        if introspector is None:
            introspector = SchemaIntrospector(ctx)
        return introspector.describe_query(self.sql)

    def register(
        self,
        ctx: SessionContext,
        *,
        record_view: Callable[[str, str | None], None] | None = None,
        validate: bool = True,
    ) -> None:
        """Register the view definition on a SessionContext.

        Parameters
        ----------
        ctx:
            DataFusion session context used for registration.
        record_view:
            Optional callback to record the view definition.
        validate:
            Whether to validate the resulting schema after registration.
        """
        ctx.sql(self.create_view_sql()).collect()
        if record_view is not None:
            record_view(self.name, self.sql)
        if validate:
            self.validate(ctx)

    def validate(self, ctx: SessionContext) -> None:
        """Validate that the view schema matches the spec.

        Parameters
        ----------
        ctx:
            DataFusion session context used for validation.

        Raises
        ------
        ViewSchemaMismatchError
            Raised when the view schema differs from the spec.
        """
        expected = _schema_signature(self.schema)
        actual = _schema_signature(self._resolve_schema(ctx))
        if actual != expected:
            msg = f"View schema mismatch for {self.name!r}."
            raise ViewSchemaMismatchError(msg)

    def _resolve_schema(self, ctx: SessionContext) -> pa.Schema:
        try:
            return ctx.table(self.name).schema()
        except (KeyError, RuntimeError, TypeError, ValueError):
            return ctx.sql(self.sql).schema()


__all__ = ["ViewSchemaMismatchError", "ViewSpec", "view_spec_from_sql"]
