"""Schema-first view specifications for DataFusion integration."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
from datafusion import SessionContext, SQLOptions
from datafusion.dataframe import DataFrame

from datafusion_engine.schema_introspection import SchemaIntrospector


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


def view_spec_from_builder(
    ctx: SessionContext,
    *,
    name: str,
    builder: Callable[[SessionContext], DataFrame],
    sql: str | None = None,
) -> ViewSpec:
    """Return a view spec derived from a DataFrame builder.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer the schema.
    name:
        Name to assign to the view spec.
    builder:
        Callable that returns a DataFusion DataFrame for the view.
    sql:
        Optional SQL definition for diagnostics.

    Returns
    -------
    ViewSpec
        View specification with the inferred schema.
    """
    schema = _schema_from_df(builder(ctx))
    return ViewSpec(name=name, sql=sql, schema=schema, builder=builder)


def _schema_from_df(df: DataFrame) -> pa.Schema:
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


@dataclass(frozen=True)
class ViewSpec:
    """Schema-validated view definition."""

    name: str
    sql: str | None
    schema: pa.Schema
    builder: Callable[[SessionContext], DataFrame] | None = None

    def describe(
        self,
        ctx: SessionContext,
        introspector: SchemaIntrospector | None = None,
        *,
        sql_options: SQLOptions | None = None,
    ) -> list[dict[str, object]]:
        """Return DESCRIBE rows for the view's query.

        Parameters
        ----------
        ctx:
            DataFusion session context used for DESCRIBE execution.
        introspector:
            Optional schema introspector to reuse existing context state.
        sql_options:
            Optional SQL options to enforce SQL execution policy.

        Returns
        -------
        list[dict[str, object]]
            ``DESCRIBE`` output rows for the view query.
        """
        if introspector is None:
            introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        if self.sql is None:
            return [
                {
                    "column_name": field.name,
                    "data_type": str(field.type),
                    "nullable": field.nullable,
                }
                for field in self.schema
            ]
        return introspector.describe_query(self.sql)

    def register(
        self,
        ctx: SessionContext,
        *,
        record_view: Callable[[str, str | None], None] | None = None,
        validate: bool = True,
        sql_options: SQLOptions | None = None,
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
        sql_options:
            Optional SQL options to enforce SQL execution policy.

        Raises
        ------
        ValueError
            Raised when SQL execution does not return a DataFrame.
        """
        if self.builder is None:
            msg = (
                f"View {self.name!r} requires a DataFrame builder; "
                "SQL-string registration is disabled."
            )
            raise ValueError(msg)
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        df = self.builder(ctx)
        adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
        adapter.register_view(self.name, df, overwrite=False, temporary=False)
        if record_view is not None:
            record_view(self.name, self.sql)
        if validate:
            self.validate(ctx, sql_options=sql_options)

    def validate(self, ctx: SessionContext, *, sql_options: SQLOptions | None = None) -> None:
        """Validate that the view schema matches the spec.

        Parameters
        ----------
        ctx:
            DataFusion session context used for validation.
        sql_options:
            Optional SQL options to enforce SQL execution policy.

        Raises
        ------
        ViewSchemaMismatchError
            Raised when the view schema differs from the spec.
        """
        expected = _schema_signature(self.schema)
        actual = _schema_signature(self._resolve_schema(ctx, sql_options=sql_options))
        if actual != expected:
            msg = f"View schema mismatch for {self.name!r}."
            raise ViewSchemaMismatchError(msg)

    def _resolve_schema(self, ctx: SessionContext, *, sql_options: SQLOptions | None) -> pa.Schema:
        _ = sql_options
        try:
            return ctx.table(self.name).schema()
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            if self.builder is not None:
                return _schema_from_df(self.builder(ctx))
            msg = f"View {self.name!r} does not define a builder."
            raise ValueError(msg) from exc


__all__ = ["ViewSchemaMismatchError", "ViewSpec", "view_spec_from_builder"]
