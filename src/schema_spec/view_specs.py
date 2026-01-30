"""DataFusion-first view specifications for registry views."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_guard import safe_sql
from validation.violations import ValidationViolation, ViolationType

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.runtime import SessionRuntime
    from datafusion_engine.schema_contracts import SchemaContract
    from datafusion_engine.sql_guard import SqlBindings


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
    return ViewSpec(
        name=inputs.name,
        schema=schema,
        builder=inputs.builder,
    )


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


@dataclass(frozen=True)
class ViewSpec:
    """DataFusion-first view definition."""

    name: str
    schema: pa.Schema | None = None
    builder: Callable[[SessionContext], DataFrame] | None = None

    def describe(
        self,
        session_runtime: SessionRuntime,
        introspector: SchemaIntrospector | None = None,
        *,
        sql_options: SQLOptions | None = None,
    ) -> list[dict[str, object]]:
        """Return DESCRIBE rows for the view's query.

        Parameters
        ----------
        session_runtime:
            DataFusion SessionRuntime used for DESCRIBE execution.
        introspector:
            Optional schema introspector to reuse existing context state.
        sql_options:
            Optional SQL options to enforce SQL execution policy.

        Returns
        -------
        list[dict[str, object]]
            ``DESCRIBE`` output rows for the view query.

        Raises
        ------
        ValueError
            Raised when the view lacks a builder and is not registered.
        """
        ctx = session_runtime.ctx
        resolved_introspector = introspector
        if resolved_introspector is None:
            try:
                resolved_introspector = SchemaIntrospector(ctx, sql_options=sql_options)
            except (RuntimeError, TypeError, ValueError):
                resolved_introspector = None
        if resolved_introspector is not None:
            describe_rows = _describe_rows_from_introspection(
                resolved_introspector,
                table_name=self.name,
            )
            if describe_rows:
                return describe_rows
        if self.builder is None:
            try:
                df = ctx.table(self.name)
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                msg = f"View {self.name!r} missing builder and registration."
                raise ValueError(msg) from exc
        else:
            df = self.builder(ctx)
        schema = _arrow_schema_from_df(df)
        return _describe_rows_from_schema(schema)

    def register(
        self,
        session_runtime: SessionRuntime,
        *,
        validate: bool = True,
        sql_options: SQLOptions | None = None,
    ) -> None:
        """Register the view definition on a SessionRuntime.

        Parameters
        ----------
        session_runtime:
            DataFusion SessionRuntime used for registration.
        validate:
            Whether to validate the resulting schema after registration.
        sql_options:
            Optional SQL options to enforce SQL execution policy.
        """
        from datafusion_engine.runtime import register_view_specs

        _ = sql_options
        register_view_specs(
            session_runtime.ctx,
            views=(self,),
            runtime_profile=session_runtime.profile,
            validate=validate,
        )

    def validate(
        self,
        session_runtime: SessionRuntime,
        *,
        sql_options: SQLOptions | None = None,
    ) -> None:
        """Validate that the view schema matches the spec.

        Parameters
        ----------
        session_runtime:
            DataFusion SessionRuntime used for validation.
        sql_options:
            Optional SQL options to enforce SQL execution policy.

        Raises
        ------
        ViewSchemaMismatchError
            Raised when the view schema differs from the spec.
        ValueError
            Raised when schema introspection is unavailable.
        """
        if self.schema is None:
            return
        ctx = session_runtime.ctx
        actual = self._resolve_schema(session_runtime, sql_options=sql_options)
        from datafusion_engine.schema_contracts import SchemaContract

        contract = SchemaContract.from_arrow_schema(self.name, self.schema)
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        snapshot = introspector.snapshot
        if snapshot is None:
            msg = "Schema introspection snapshot unavailable for view validation."
            raise ValueError(msg)
        violations = contract.validate_against_introspection(snapshot)
        violations.extend(_schema_metadata_violations(actual, contract))
        if violations:
            msg = f"View schema mismatch for {self.name!r}."
            raise ViewSchemaMismatchError(msg)

    def _resolve_schema(
        self,
        session_runtime: SessionRuntime,
        *,
        sql_options: SQLOptions | None,
    ) -> pa.Schema:
        _ = sql_options
        ctx = session_runtime.ctx
        try:
            return _arrow_schema_from_df(ctx.table(self.name))
        except (KeyError, RuntimeError, TypeError, ValueError) as exc:
            if self.builder is not None:
                return _arrow_schema_from_df(self.builder(ctx))
            msg = f"View {self.name!r} does not define a builder."
            raise ValueError(msg) from exc


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


def _describe_rows_from_schema(schema: pa.Schema) -> list[dict[str, object]]:
    return [
        {
            "column_name": field.name,
            "data_type": str(field.type),
            "nullable": field.nullable,
            "source": "arrow_schema",
        }
        for field in schema
    ]


def _nullable_from_value(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"yes", "true", "1", "y"}:
        return True
    if text in {"no", "false", "0", "n"}:
        return False
    return None


def _describe_rows_from_introspection(
    introspector: SchemaIntrospector,
    *,
    table_name: str,
) -> list[dict[str, object]]:
    rows = introspector.table_columns(table_name)
    if not rows:
        return []
    normalized: list[dict[str, object]] = []
    for row in rows:
        column_name = row.get("column_name")
        if column_name is None:
            continue
        data_type = row.get("data_type")
        nullable = _nullable_from_value(row.get("is_nullable") or row.get("nullable"))
        normalized.append(
            {
                "column_name": str(column_name),
                "data_type": str(data_type) if data_type is not None else None,
                "nullable": nullable,
                "source": "information_schema",
            }
        )
    return normalized


def _schema_metadata_violations(
    schema: pa.Schema,
    contract: SchemaContract,
) -> list[ValidationViolation]:
    from datafusion_engine.schema_contracts import SCHEMA_ABI_FINGERPRINT_META

    expected = contract.schema_metadata or {}
    if not expected:
        return []
    actual = schema.metadata or {}
    violations: list[ValidationViolation] = []
    expected_abi = expected.get(SCHEMA_ABI_FINGERPRINT_META)
    if expected_abi is not None:
        actual_abi = schema_identity_hash(schema).encode("utf-8")
        if actual_abi != expected_abi:
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.METADATA_MISMATCH,
                    table_name=contract.table_name,
                    column_name=SCHEMA_ABI_FINGERPRINT_META.decode("utf-8"),
                    expected=expected_abi.decode("utf-8", errors="replace"),
                    actual=actual_abi.decode("utf-8", errors="replace"),
                )
            )
    for key, expected_value in expected.items():
        if key == SCHEMA_ABI_FINGERPRINT_META:
            continue
        actual_value = actual.get(key)
        if actual_value is None or actual_value == expected_value:
            continue
        violations.append(
            ValidationViolation(
                violation_type=ViolationType.METADATA_MISMATCH,
                table_name=contract.table_name,
                column_name=key.decode("utf-8", errors="replace"),
                expected=expected_value.decode("utf-8", errors="replace"),
                actual=actual_value.decode("utf-8", errors="replace"),
            )
        )
    return violations


__all__ = ["ViewSchemaMismatchError", "ViewSpec", "view_spec_from_builder", "view_spec_from_sql"]
