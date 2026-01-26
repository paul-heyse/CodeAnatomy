"""SQLGlot AST-first DDL/COPY generation for DataFusion SQL statements.

This module provides AST-based builders for DDL and DML statements,
replacing string assembly with structured SQLGlot expression building.
All builders produce SQLGlot expressions that can be rendered to
dialect-specific SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlglot_tools.compat import exp
from sqlglot_tools.optimizer import (
    ExternalTableCompressionProperty,
    ExternalTableOptionsProperty,
    ExternalTableOrderProperty,
)

if TYPE_CHECKING:
    from ibis_engine.registry import DatasetLocation


@dataclass(frozen=True)
class ExternalTableDDLConfig:
    """Configuration for external table DDL generation.

    Parameters
    ----------
    schema
        Explicit column definitions (name -> type).
    schema_expressions
        SQLGlot schema expressions (ColumnDef + constraints) when available.
    file_format
        File format (PARQUET, CSV, JSON, ARROW).
    options
        Format-specific options for the table provider.
    compression
        Compression type for external table registration.
    partitioned_by
        Partition columns for hive-style partitioning.
    file_sort_order
        File sort order columns for DataFusion listing tables.
    unbounded
        Whether to mark the table as UNBOUNDED EXTERNAL (streaming).
    dialect
        SQL dialect name for rendering.
    """

    schema: dict[str, str] | None = None
    schema_expressions: list[exp.Expression] | None = None
    file_format: str = "PARQUET"
    options: dict[str, str] | None = None
    compression: str | None = None
    partitioned_by: tuple[str, ...] | None = None
    file_sort_order: tuple[tuple[str, str], ...] | None = None
    unbounded: bool = False
    dialect: str | None = "datafusion"


@dataclass(frozen=True)
class FunctionArgSpec:
    """Argument definition for CREATE FUNCTION statements."""

    name: str
    dtype: str


@dataclass(frozen=True)
class CreateFunctionConfig:
    """Configuration for CREATE FUNCTION statements."""

    name: str
    args: tuple[FunctionArgSpec, ...] = ()
    return_type: str | None = None
    returns_table: bool = False
    body: exp.Expression | None = None
    language: str | None = None
    volatility: str | None = None
    replace: bool = False


def build_create_function_ast(*, config: CreateFunctionConfig) -> exp.Create:
    """Build CREATE FUNCTION statements as SQLGlot AST.

    Parameters
    ----------
    config
        Function configuration including name, args, return type, and body.

    Returns
    -------
    exp.Create
        SQLGlot CREATE FUNCTION expression.

    Raises
    ------
    ValueError
        Raised when the function body is missing or the return type is invalid.
    """
    if config.body is None:
        msg = "CREATE FUNCTION requires a function body expression."
        raise ValueError(msg)
    return_type = config.return_type
    if return_type is None and not config.returns_table:
        msg = "CREATE FUNCTION requires a return type or returns_table=True."
        raise ValueError(msg)
    args = [
        exp.ColumnDef(
            this=exp.to_identifier(arg.name),
            kind=exp.DataType.build(arg.dtype),
        )
        for arg in config.args
    ]
    udf = exp.UserDefinedFunction(
        this=exp.Table(this=exp.to_identifier(config.name)),
        expressions=args,
        wrapped=True,
    )
    properties: list[exp.Expression] = []
    if config.returns_table:
        properties.append(exp.ReturnsProperty(this=exp.Var(this="TABLE"), is_table=True))
    else:
        if return_type is None:
            msg = "CREATE FUNCTION requires a return type when returns_table is False."
            raise ValueError(msg)
        properties.append(exp.ReturnsProperty(this=exp.DataType.build(return_type)))
    if config.language:
        properties.append(exp.LanguageProperty(this=exp.Var(this=config.language.upper())))
    if config.volatility:
        properties.append(exp.StabilityProperty(this=exp.Literal.string(config.volatility.upper())))
    return exp.Create(
        this=udf,
        kind="FUNCTION",
        expression=config.body,
        properties=exp.Properties(expressions=properties),
        replace=config.replace,
    )


def build_copy_to_ast(
    *,
    query: exp.Expression,
    path: str,
    file_format: str | None = "PARQUET",
    options: dict[str, object] | None = None,
    partition_by: tuple[str, ...] = (),
) -> exp.Copy:
    r"""
    Build COPY TO statement as SQLGlot AST.

    Constructs a COPY TO expression for writing query results to a file
    or object store location. Supports format-specific options, compression
    settings, and Hive-style partitioning.

    Parameters
    ----------
    query
        The source query expression to copy from.
    path
        Destination path (local filesystem or object store URI).
    file_format
        Output format (PARQUET, CSV, JSON, ARROW).
    options
        Format-specific options (compression, delimiter, etc.).
    partition_by
        Columns for Hive-style partitioning.

    Returns
    -------
    exp.Copy
        SQLGlot COPY expression ready for dialect rendering.

    Examples
    --------
    >>> from sqlglot_tools.optimizer import parse_sql_strict
    >>> query = parse_sql_strict("SELECT * FROM mytable", dialect="datafusion")
    >>> copy_expr = build_copy_to_ast(
    ...     query=query,
    ...     path="/tmp/output.parquet",
    ...     file_format="PARQUET",
    ...     options={"compression": "zstd"},
    ...     partition_by=("year", "month"),
    ... )
    >>> copy_expr.sql(dialect="postgres")
    'COPY (SELECT * FROM mytable) TO \'/tmp/output.parquet\' ...'
    """
    option_exprs: list[exp.Expression] = []

    # Format option (omit if None)
    if file_format:
        option_exprs.append(
            exp.Property(this=exp.Var(this="FORMAT"), value=exp.Var(this=file_format))
        )

    # User-provided options (ignore explicit format override)
    for key, value in (options or {}).items():
        if str(key).lower() == "format":
            continue
        option_exprs.append(
            exp.Property(
                this=exp.Var(this=key.upper()),
                value=exp.Literal.string(str(value)),
            )
        )

    copy_expr = exp.Copy(
        this=exp.Subquery(this=query),
        kind=exp.Var(this="TO"),
        files=[exp.Literal.string(path)],
        options=option_exprs,
    )

    # Add PARTITIONED BY if specified
    if partition_by:
        copy_expr.set(
            "partition",
            [exp.to_identifier(col) for col in partition_by],
        )

    return copy_expr


def build_external_table_ddl(
    *,
    name: str,
    location: DatasetLocation,
    config: ExternalTableDDLConfig | None = None,
) -> exp.Expression:
    """
    Build CREATE EXTERNAL TABLE DDL via SQLGlot AST.

    Constructs a CREATE EXTERNAL TABLE statement for registering
    external datasets in DataFusion. Supports schema inference,
    explicit column definitions, and format-specific options.

    Parameters
    ----------
    name
        Table name for registration.
    location
        Dataset location metadata including path and storage options.
    config
        Configuration for schema, format, options, and SQL dialect.

    Returns
    -------
    sqlglot.expressions.Expression
        CREATE EXTERNAL TABLE AST.

    Examples
    --------
    >>> from ibis_engine.registry import DatasetLocation
    >>> location = DatasetLocation(path="/data/events.parquet")
    >>> config = ExternalTableDDLConfig(
    ...     schema={"id": "BIGINT", "timestamp": "TIMESTAMP"},
    ...     file_format="PARQUET",
    ... )
    >>> ddl = build_external_table_ddl(name="events", location=location, config=config)
    >>> print(ddl.sql(dialect="datafusion"))
    CREATE EXTERNAL TABLE events (id BIGINT, timestamp TIMESTAMP) ...
    """
    ddl_config = config or ExternalTableDDLConfig()
    file_format = ddl_config.file_format

    # Build column definitions if schema provided
    columns = None
    if ddl_config.schema_expressions:
        columns = exp.Schema(expressions=list(ddl_config.schema_expressions))
    elif ddl_config.schema:
        columns = exp.Schema(
            expressions=[
                exp.ColumnDef(
                    this=exp.to_identifier(col_name),
                    kind=exp.DataType.build(col_type),
                )
                for col_name, col_type in ddl_config.schema.items()
            ]
        )

    stored_as = "DELTATABLE" if file_format.lower() == "delta" else file_format.upper()
    properties: list[exp.Expression] = [
        exp.FileFormatProperty(this=exp.Var(this=stored_as)),
        exp.LocationProperty(this=exp.Literal.string(str(location.path))),
    ]
    if ddl_config.compression:
        properties.append(
            ExternalTableCompressionProperty(this=exp.Var(this=ddl_config.compression))
        )
    if ddl_config.partitioned_by:
        properties.append(
            exp.PartitionedByProperty(
                this=exp.Tuple(
                    expressions=[exp.Identifier(this=name) for name in ddl_config.partitioned_by]
                )
            )
        )
    if ddl_config.file_sort_order:
        order_exprs: list[exp.Expression] = []
        for item in ddl_config.file_sort_order:
            if isinstance(item, str):
                column_name = item
                direction = "ascending"
            else:
                column_name, direction = item
            desc = str(direction).lower() in {"desc", "descending"}
            order_exprs.append(exp.Ordered(this=exp.Identifier(this=column_name), desc=desc))
        properties.append(ExternalTableOrderProperty(expressions=order_exprs))
    options_property = _external_table_options_property(ddl_config.options)
    if options_property is not None:
        properties.append(options_property)

    # Build CREATE EXTERNAL TABLE expression
    return exp.Create(
        this=exp.Table(this=exp.to_identifier(name)),
        kind="UNBOUNDED EXTERNAL" if ddl_config.unbounded else "EXTERNAL",
        expression=columns,
        properties=exp.Properties(expressions=properties),
    )


def _external_table_options_property(
    options: dict[str, str] | None,
) -> ExternalTableOptionsProperty | None:
    if not options:
        return None
    entries: list[exp.Expression] = []
    for key, value in options.items():
        rendered = _option_literal_value(value)
        if rendered is None:
            continue
        entries.append(
            exp.Tuple(
                expressions=[
                    exp.Literal.string(str(key)),
                    exp.Literal.string(rendered),
                ]
            )
        )
    if not entries:
        return None
    return ExternalTableOptionsProperty(expressions=entries)


def _option_literal_value(value: object) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def build_insert_into_ast(
    *,
    target_table: str,
    source_query: exp.Expression,
    columns: tuple[str, ...] | None = None,
    overwrite: bool = False,
) -> exp.Insert:
    """
    Build INSERT INTO statement as SQLGlot AST.

    Constructs an INSERT INTO expression for loading query results
    into a target table. Supports column list specification and
    overwrite mode.

    Parameters
    ----------
    target_table
        Name of the target table.
    source_query
        Source query expression to insert from.
    columns
        Optional column list for targeted insertion.
    overwrite
        If True, replace existing table contents (INSERT OVERWRITE).

    Returns
    -------
    exp.Insert
        SQLGlot INSERT expression ready for dialect rendering.

    Examples
    --------
    >>> from sqlglot_tools.compat import parse_one
    >>> source = parse_one("SELECT id, name FROM staging")
    >>> insert_expr = build_insert_into_ast(
    ...     target_table="production",
    ...     source_query=source,
    ...     columns=("id", "name"),
    ... )
    >>> insert_expr.sql(dialect="postgres")
    'INSERT INTO production (id, name) SELECT id, name FROM staging'
    """
    insert_expr = exp.Insert(
        this=exp.Table(this=exp.to_identifier(target_table)),
        expression=source_query,
        overwrite=overwrite,
    )

    if columns:
        insert_expr.set(
            "columns",
            [exp.to_identifier(col) for col in columns],
        )

    return insert_expr


__all__ = [
    "CreateFunctionConfig",
    "FunctionArgSpec",
    "build_copy_to_ast",
    "build_create_function_ast",
    "build_external_table_ddl",
    "build_insert_into_ast",
]
