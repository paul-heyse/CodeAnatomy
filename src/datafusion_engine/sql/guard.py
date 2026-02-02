"""SQL guardrails for DataFusion planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext, SQLOptions

from datafusion_engine.sql.options import safe_sql_options_for_profile

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.tables.param import DataFusionParamBindings


@dataclass(frozen=True)
class SqlBindings:
    """Parameter bindings for SQL execution.

    Attributes
    ----------
    param_values
        Scalar parameter values passed via ``param_values``.
    named_params
        Named parameters passed via keyword arguments.
    """

    param_values: Mapping[str, object] | None = None
    named_params: Mapping[str, object] | None = None


@dataclass(frozen=True)
class _ResolvedSqlBindings:
    """Resolved and validated SQL parameter bindings."""

    param_values: dict[str, object]
    named_params: dict[str, object]
    table_bindings: DataFusionParamBindings | None


def safe_sql(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    bindings: SqlBindings | None = None,
) -> DataFrame:
    """Return a DataFrame for SQL using SessionConfig policy enforcement.

    Parameters
    ----------
    ctx
        DataFusion session context used for SQL execution.
    sql
        SQL string to execute.
    sql_options
        Optional SQL options overriding the default SessionConfig-driven policy.
    runtime_profile
        Optional runtime profile for SQL policy resolution.
    bindings
        Optional parameter bindings for positional and named parameters.

    Returns
    -------
    DataFrame
        DataFusion DataFrame resulting from SQL execution.

    Raises
    ------
    ValueError
        Raised when parameter bindings are invalid or SQL execution fails.
    """
    options = _resolve_sql_options(sql_options, runtime_profile=runtime_profile)
    resolved = _resolve_bindings(bindings)
    df = _execute_sql(ctx, sql, options=options, bindings=resolved)
    if df is None:
        msg = "SQL execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return df


def _resolve_sql_options(
    sql_options: SQLOptions | None,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> SQLOptions:
    """Resolve SQL options for SQL execution.

    Returns
    -------
    SQLOptions
        SQL options resolved from explicit overrides or defaults.
    """
    if sql_options is not None:
        return sql_options
    return safe_sql_options_for_profile(runtime_profile)


def _resolve_bindings(bindings: SqlBindings | None) -> _ResolvedSqlBindings:
    """Resolve and validate SQL parameter bindings.

    Returns
    -------
    _ResolvedSqlBindings
        Normalized bindings used for SQL execution.
    """
    resolved = bindings or SqlBindings()
    param_values = _resolve_param_values(resolved.param_values)
    named_params, table_bindings = _resolve_named_params(
        resolved.named_params,
        existing_param_values=param_values,
    )
    return _ResolvedSqlBindings(
        param_values=param_values,
        named_params=named_params,
        table_bindings=table_bindings,
    )


def _resolve_param_values(values: Mapping[str, object] | None) -> dict[str, object]:
    """Resolve scalar parameter values for SQL execution.

    Returns
    -------
    dict[str, object]
        Scalar parameter values ready for DataFusion execution.

    Raises
    ------
    ValueError
        Raised when table-like params are passed via ``param_values``.
    """
    if not values:
        return {}
    from datafusion_engine.tables.param import resolve_param_bindings

    param_bindings = resolve_param_bindings(values, validate_names=False)
    if param_bindings.named_tables:
        msg = "Table-like parameters must be passed via named_params."
        raise ValueError(msg)
    return dict(param_bindings.param_values)


def _resolve_named_params(
    values: Mapping[str, object] | None,
    *,
    existing_param_values: Mapping[str, object],
) -> tuple[dict[str, object], DataFusionParamBindings | None]:
    """Resolve named parameters and any table bindings.

    Returns
    -------
    tuple[dict[str, object], DataFusionParamBindings | None]
        Scalar named parameters and optional table bindings.

    Raises
    ------
    ValueError
        Raised when duplicate bindings are provided.
    """
    if not values:
        return {}, None
    from datafusion_engine.tables.param import resolve_param_bindings

    named_bindings = resolve_param_bindings(values)
    overlap = set(named_bindings.param_values) & set(existing_param_values)
    if overlap:
        msg = f"Duplicate parameter bindings: {sorted(overlap)}."
        raise ValueError(msg)
    named_scalar_params = dict(named_bindings.param_values)
    table_bindings = named_bindings if named_bindings.named_tables else None
    return named_scalar_params, table_bindings


def _execute_sql(
    ctx: SessionContext,
    sql: str,
    *,
    options: SQLOptions,
    bindings: _ResolvedSqlBindings,
) -> DataFrame | None:
    """Execute SQL with resolved bindings and guardrails.

    Returns
    -------
    DataFrame | None
        DataFusion DataFrame or ``None`` if execution returned nothing.

    Raises
    ------
    ValueError
        Raised when SQL execution fails.
    """
    from datafusion_engine.tables.param import register_table_params

    try:
        if bindings.table_bindings is None:
            return ctx.sql_with_options(
                sql,
                options,
                param_values=bindings.param_values or None,
                **bindings.named_params,
            )
        with register_table_params(ctx, bindings.table_bindings):
            return ctx.sql_with_options(
                sql,
                options,
                param_values=bindings.param_values or None,
                **bindings.named_params,
            )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = "SQL execution failed under safe options."
        raise ValueError(msg) from exc


__all__ = ["SqlBindings", "safe_sql"]
